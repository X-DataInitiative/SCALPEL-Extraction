package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{Column, Dataset}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.computeDateUsingMonthYear

case class PatientHad(patientID: String, exitMode: Int, exitMonth: String, exitYear: String, gender: Int, birthDate: Timestamp, deathDate: Option[Timestamp])
  extends DerivedPatient

private[patients] object HadPatients extends PatientExtractor[PatientHad] {

  /** Find birth date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with birth date.
   */
  override def findPatientBirthDate(patients: Dataset[PatientHad]): Dataset[PatientHad] = {
    patients
  }

  /** Find gender of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with gender.
   */
  override def findPatientGender(patients: Dataset[PatientHad]): Dataset[PatientHad] = {
    patients
  }

  /** Find death date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with death date.
   */
  override def findPatientDeathDate(patients: Dataset[PatientHad]): Dataset[PatientHad] = {
    import patients.sparkSession.implicits._
    val deathCode = 9
    patients
      .filter(_.exitMode == deathCode)
      .groupByKey(p => p.patientID)
      .reduceGroups((p1, p2) => if ((p2.deathDate.isEmpty && p1.deathDate.isEmpty) || p2.deathDate.isEmpty || (p1.deathDate.isDefined && p1.deathDate.get.before(p2.deathDate.get))) p1 else p2)
      .map(p =>
        PatientHad(
          p._2.patientID,
          p._2.exitMode,
          p._2.exitMonth,
          p._2.exitYear,
          p._2.gender,
          p._2.birthDate,
          p._2.deathDate
        ))
  }

  /** Gets and prepares all the needed columns from the Sources.
   *
   * @param sources Source object [[Sources]] that contains all sources.
   * @return A [[Dataset]] with needed columns.
   */
  override def getInput(sources: Sources): Dataset[PatientHad] = {
    val inputColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("HAD_B__SOR_MOD").cast(IntegerType).as("exitMode"),
      col("HAD_B__SOR_MOI").cast(StringType).as("exitMonth"),
      col("HAD_B__SOR_ANN").cast(StringType).as("exitYear"),
      lit(0).cast(IntegerType).as("gender"),
      lit(null).cast(TimestampType).as("birthDate"),
      computeDateUsingMonthYear(col("HAD_B__SOR_MOI"), col("HAD_B__SOR_ANN")).cast(TimestampType).as("deathDate")
    )

    val had = sources.had.get
    import had.sqlContext.implicits._
    had.select(inputColumns: _*).as[PatientHad]
  }
}
