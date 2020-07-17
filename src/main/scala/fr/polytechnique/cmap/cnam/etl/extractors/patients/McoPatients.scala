// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{Column, Dataset}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.computeDateUsingMonthYear

case class PatientMco(patientID: String, exitMode: Int, exitMonth: String, exitYear: String, gender: Int, birthDate: Timestamp, deathDate: Option[Timestamp])
  extends DerivedPatient

private[patients] object McoPatients extends PatientExtractor[PatientMco] {

  /** Find birth date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with birth date.
   */
  override def findPatientBirthDate(patients: Dataset[PatientMco]): Dataset[PatientMco] = {
    patients
  }

  /** Find gender of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with gender.
   */
  override def findPatientGender(patients: Dataset[PatientMco]): Dataset[PatientMco] = {
    patients
  }

  /** Find death date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with death date.
   */
  override def findPatientDeathDate(patients: Dataset[PatientMco]): Dataset[PatientMco] = {
    import patients.sparkSession.implicits._
    val deathCode = 9
    patients
      .filter(_.exitMode == deathCode)
      .groupByKey(p => p.patientID)
      .reduceGroups((p1, p2) => if ((p2.deathDate.isEmpty && p1.deathDate.isEmpty) || p2.deathDate.isEmpty || (p1.deathDate.isDefined && p1.deathDate.get.before(p2.deathDate.get))) p1 else p2)
      .map(p =>
        PatientMco(
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
  override def getInput(sources: Sources): Dataset[PatientMco] = {
    val inputColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("MCO_B__SOR_MOD").cast(IntegerType).as("exitMode"),
      col("SOR_MOI").cast(StringType).as("exitMonth"),
      col("SOR_ANN").cast(StringType).as("exitYear"),
      lit(0).cast(IntegerType).as("gender"),
      lit(null).cast(TimestampType).as("birthDate"),
      computeDateUsingMonthYear(col("SOR_MOI"), col("SOR_ANN")).cast(TimestampType).as("deathDate")
    )

    val mco = sources.mco.get
    import mco.sqlContext.implicits._
    mco.select(inputColumns: _*).as[PatientMco]
  }
}
