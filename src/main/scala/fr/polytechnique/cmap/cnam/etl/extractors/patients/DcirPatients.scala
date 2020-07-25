// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.PatientUtils._
import fr.polytechnique.cmap.cnam.etl.sources.Sources

case class PatientDcir(patientID: String, gender: Int, age: Int, birthYear: String, birthDate: Timestamp, eventDate: Timestamp, deathDate: Option[Timestamp])
  extends DerivedPatient

private[patients] object DcirPatients extends PatientExtractor[PatientDcir] {

  /** Find birth date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with birth date.
   */
  override def findPatientBirthDate(patients: Dataset[PatientDcir]): Dataset[PatientDcir] = {

    val window = Window.partitionBy(col("patientID")).orderBy(col("count").desc, col("birthYear"))
    val birthYear = patients
      .groupBy(col("patientID"), col("birthYear")).agg(count("*").as("count"))
      // "first" is only deterministic when applied over an ordered window:
      .select(col("patientID"), first(col("birthYear")).over(window).as("birthYear"))
      .distinct

    // After selecting the data, the next step is to group by patientId and age, because we need to
    //   estimate the birthDate ant we use min(eventDate) and max(eventDate) for each age to achieve
    //   that.
    val minmaxevent = patients
      .groupBy(col("patientID"), col("age"))
      .agg(
        min("eventDate").as("minEventDate"), // the min event date for each age of a patient
        max("eventDate").as("maxEventDate") // the max event date for each age of a patient
      )

    // Then we aggregate again by taking the mean between the closest dates where the age changed.
    // For example, if the patient was 60yo when an event happened on Apr/2010 and he was 61yo when
    //   another event happened on Jun/2010, we calculate the mean and estimate his birthday as
    //   being in May of the year found
    val birthDateAggCol: Column = estimateBirthDateCol(
      max(col("minEventDate")).cast(TimestampType),
      min(col("maxEventDate")).cast(TimestampType),
      first(col("birthYear"))
    )

    val birthDate = minmaxevent.join(birthYear, "patientID")
      .groupBy(col("patientID"))
      .agg(
        birthDateAggCol.as("birthDate"))

    import patients.sparkSession.implicits._

    val result = patients.as("patients")
      .joinWith(birthYear.as("birthYearDf"), col("patients.patientID").equalTo(col("birthYearDf.patientID")), "left")

    result
      .joinWith(birthDate, result("_1.patientID") === birthDate("patientID"), "left")
      .map(p =>
        PatientDcir(
          p._1._1.patientID,
          p._1._1.gender,
          p._1._1.age,
          p._1._2.getAs("birthYear"),
          p._2.getAs("birthDate"),
          p._1._1.eventDate,
          p._1._1.deathDate
        ))
  }

  /** Find gender of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with gender.
   */
  override def findPatientGender(patients: Dataset[PatientDcir]): Dataset[PatientDcir] = {

    import patients.sparkSession.implicits._

    val genderCodeError = 9

    val gendercount = patients
      .filter(_.gender != genderCodeError)
      .groupByKey(p => (p.patientID, p.age))
      .count()
      .map(p => (p._1._1, p._2.toInt))

    val gendersum = patients
      .filter(_.gender != genderCodeError)
      .map(p => ((p.patientID, p.age), p.gender))
      .groupByKey(_._1)
      .mapValues(row => row._2)
      .reduceGroups((acc, str) => acc + str)
      .map(p => (p._1, p._2))

    val sumgendercount = gendercount
      .groupByKey(p => p._1)
      .mapValues(row => row._2)
      .reduceGroups((acc, str) => acc + str)
      .map(p => (p._1, p._2))

    val sumgendersum = gendersum
      .groupByKey(p => p._1._1)
      .mapValues(row => row._2)
      .reduceGroups((acc, str) => acc + str)
      .map(p => (p._1, p._2))

    val result = patients.joinWith(sumgendersum, patients("patientID") === sumgendersum("_1"), "left")

    result.joinWith(sumgendercount, result("_1.patientID") === sumgendercount("_1"), "left")
      .map(p =>
        PatientDcir(
          p._1._1.patientID,
          if(p._2 != null) Math.round(p._1._2._2.toFloat / p._2._2) else 0,
          p._1._1.age,
          p._1._1.birthYear,
          p._1._1.birthDate,
          p._1._1.eventDate,
          p._1._1.deathDate
        ))
  }

  /** Find death date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with death date.
   */
  override def findPatientDeathDate(patients: Dataset[PatientDcir]): Dataset[PatientDcir] = {
    import patients.sparkSession.implicits._
    val mindeathdate = patients
      .groupByKey(p => p.patientID)
      .reduceGroups((p1, p2) => if ((p2.deathDate.isEmpty && p1.deathDate.isEmpty) || p2.deathDate.isEmpty || (p1.deathDate.isDefined && p1.deathDate.get.before(p2.deathDate.get))) p1 else p2)
      .map(p => (p._2.patientID, p._2.deathDate))

    val genderCodeError = 9

    patients
      .filter(_.gender != genderCodeError)
      .joinWith(mindeathdate, patients("patientID") === mindeathdate("_1"), "left")
      .map(p =>
        PatientDcir(
          p._1.patientID,
          p._1.gender,
          p._1.age,
          p._1.birthYear,
          p._1.birthDate,
          p._1.eventDate,
          p._2._2))
  }

  /** Gets and prepares all the needed columns from the Sources.
   *
   * @param sources Source object [[Sources]] that contains all sources.
   * @return A [[Dataset]] with needed columns.
   */
  override def getInput(sources: Sources): Dataset[PatientDcir] = {
    val inputColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("BEN_SEX_COD").cast(IntegerType).as("gender"),
      col("BEN_AMA_COD").cast(IntegerType).as("age"),
      col("BEN_NAI_ANN").cast(StringType).as("birthYear"),
      lit(null).cast(TimestampType).as("birthDate"),
      col("EXE_SOI_DTD").cast(TimestampType).as("eventDate"),
      col("BEN_DCD_DTE").cast(TimestampType).as("deathDate")
    )

    val dcir = sources.dcir.get
    import dcir.sqlContext.implicits._
    dcir.select(inputColumns: _*).as[PatientDcir]
  }

}
