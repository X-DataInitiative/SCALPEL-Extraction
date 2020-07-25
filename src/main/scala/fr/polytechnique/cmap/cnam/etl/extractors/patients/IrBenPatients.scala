// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{Column, Dataset}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.computeDateUsingMonthYear

case class PatientIrBen(patientID: String, gender: Int, birthMonth: String, birthYear: String, birthDate: Timestamp, deathDate: Option[Timestamp])
  extends DerivedPatient

private[patients] object IrBenPatients extends PatientExtractor[PatientIrBen] {

  /** Find birth date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with birth date.
   */
  override def findPatientBirthDate(patients: Dataset[PatientIrBen]): Dataset[PatientIrBen] = {
    patients
  }

  /** Find gender of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with gender.
   */
  override def findPatientGender(patients: Dataset[PatientIrBen]): Dataset[PatientIrBen] = {
    patients
  }

  /** Find death date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with death date.
   */
  override def findPatientDeathDate(patients: Dataset[PatientIrBen]): Dataset[PatientIrBen] = {
    import patients.sparkSession.implicits._
    val mindeathdate = patients
      .groupByKey(p => p.patientID)
      .reduceGroups((p1, p2) => if ((p2.deathDate.isEmpty && p1.deathDate.isEmpty) || p2.deathDate.isEmpty || (p1.deathDate.isDefined && p1.deathDate.get.before(p2.deathDate.get))) p1 else p2)
      .map(p => (p._2.patientID, p._2.deathDate))

    patients.joinWith(mindeathdate, patients("patientID") === mindeathdate("_1"), "left")
      .map(p =>
        PatientIrBen(
          p._1.patientID,
          p._1.gender,
          p._1.birthMonth,
          p._1.birthYear,
          p._1.birthDate,
          p._2._2
        ))
  }

  /** Gets and prepares all the needed columns from the Sources.
   *
   * @param sources Source object [[Sources]] that contains all sources.
   * @return A [[Dataset]] with needed columns.
   */
  override def getInput(sources: Sources): Dataset[PatientIrBen] = {
    val inputColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("BEN_SEX_COD").cast(IntegerType).as("gender"),
      col("BEN_NAI_MOI").cast(StringType).as("birthMonth"),
      col("BEN_NAI_ANN").cast(StringType).as("birthYear"),
      computeDateUsingMonthYear(col("BEN_NAI_MOI"), col("BEN_NAI_ANN")).as("birthDate"),
      col("BEN_DCD_DTE").cast(TimestampType).as("deathDate")
    )

    val irBen = sources.irBen.get
    import irBen.sqlContext.implicits._
    irBen.select(inputColumns: _*).as[PatientIrBen]
  }
}
