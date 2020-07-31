// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{Column, Dataset}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.PatientUtils.estimateBirthDateCol
import fr.polytechnique.cmap.cnam.etl.sources.Sources

case class PatientMcoce(patientID: String, gender: Int, age: Option[Int], birthDate: Timestamp, eventDate: Timestamp, deathDate: Option[Timestamp])
  extends DerivedPatient


private[patients] object McocePatients extends PatientExtractor[PatientMcoce] {

  /** Find birth date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with birth date.
   */
  override def findPatientBirthDate(patients: Dataset[PatientMcoce]): Dataset[PatientMcoce] = {

    val window = Window.partitionBy(col("patientID"))
    val birthYear = min(col("eventYear") - col("age"))
      .over(window)
      .as("birthYear")

    val patientsbirthYear = patients.groupBy("patientID", "age")
      .agg(max(year(col("eventDate"))).as("eventYear"))
      .select(col("patientID"), birthYear)
      .distinct

    val minmaxevent = patients
      .groupBy(col("patientID"), col("age"))
      .agg(
        min("eventDate").as("minEventDate"), // the min event date for each age of a patient
        max("eventDate").as("maxEventDate") // the max event date for each age of a patient
      )

    val birthDateAggCol: Column = estimateBirthDateCol(
      max(col("minEventDate")).cast(TimestampType),
      min(col("maxEventDate")).cast(TimestampType),
      first(col("birthYear"))
    )

    val birthDate = minmaxevent.join(patientsbirthYear, "patientID")
      .groupBy(col("patientID"))
      .agg(
        birthDateAggCol.as("birthDate"))

    import patients.sparkSession.implicits._

    patients.as(("patients"))
      .joinWith(birthDate.as("birthDateDf"), col("patients.patientID").equalTo(col("birthDateDf.patientID")), "left")
      .map(p =>
        PatientMcoce(
          p._1.patientID,
          p._1.gender,
          p._1.age,
          p._2.getAs("birthDate"),
          p._1.eventDate,
          p._1.deathDate
        ))
  }

  /** Find gender of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with gender.
   */
  override def findPatientGender(patients: Dataset[PatientMcoce]): Dataset[PatientMcoce] = {

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
        PatientMcoce(
          p._1._1.patientID,
          if (p._2 != null) Math.round(p._1._2._2.toFloat / p._2._2) else 0,
          p._1._1.age,
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
  override def findPatientDeathDate(patients: Dataset[PatientMcoce]): Dataset[PatientMcoce] = {
    patients
  }

  /** Gets and prepares all the needed columns from the Sources.
   *
   * @param sources Source object [[Sources]] that contains all sources.
   * @return A [[Dataset]] with needed columns.
   */
  override def getInput(sources: Sources): Dataset[PatientMcoce] = {
    val inputColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("MCO_FASTC__COD_SEX").cast(IntegerType).as("gender"),
      col("MCO_FASTC__AGE_ANN").cast(IntegerType).as("age"),
      lit(null).cast(TimestampType).as("birthDate"),
      col("EXE_SOI_DTD").cast(TimestampType).as("eventDate"),
      lit(null).cast(TimestampType).as("deathDate")
    )

    val mcoce = sources.mcoCe.get
    import mcoce.sqlContext.implicits._
    mcoce.select(inputColumns: _*).as[PatientMcoce]
  }
}
