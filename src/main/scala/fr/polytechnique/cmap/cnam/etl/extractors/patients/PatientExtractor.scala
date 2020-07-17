package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait DerivedPatient {
  val patientID: String
  val gender: Int
  val birthDate: Timestamp
  val deathDate: Option[Timestamp]
}

trait PatientExtractor[PatientType <: DerivedPatient] {

  /** Find birth date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with birth date.
   */
  def findPatientBirthDate(patients: Dataset[PatientType]): Dataset[PatientType]

  /** Find gender of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with gender.
   */
  def findPatientGender(patients: Dataset[PatientType]): Dataset[PatientType]

  /** Find death date of patients.
   *
   * @param patients that contains all patients.
   * @return A [[Dataset]] of patients with death date.
   */
  def findPatientDeathDate(patients: Dataset[PatientType]): Dataset[PatientType]

  /** Transform patientBIS to patient.
   *
   * @param patients that contains all patientsBIS.
   * @return A [[Dataset]] with needed columns of Patient.
   */
  def fromDerivedPatienttoPatient(patients: Dataset[PatientType]): Dataset[Patient] = {
    val outputColumns: List[Column] = List(
      col("patientID"),
      col("gender"),
      col("birthDate"),
      col("deathDate")
    )

    import patients.sqlContext.implicits._
    patients.select(outputColumns: _*).as[Patient]
  }

  /** Gets and prepares all the needed columns from the Sources.
   *
   * @param sources Source object [[Sources]] that contains all sources.
   * @return A [[Dataset]] with needed columns.
   */
  def getInput(sources: Sources): Dataset[PatientType]

  /** Extracts the Patient from the Source.
   *
   * This function is responsible for gluing different other parts of the Extractor.
   * This method should be considered the unique callable method from a Study perspective.
   *
   * @param sources Source object [[Sources]] that contains all sources.
   * @return A Dataset of Patient of type EventType.
   */
  def extract(sources: Sources): Dataset[Patient] = {
    val input: Dataset[PatientType] = getInput(sources)

    input.transform(findPatientBirthDate)
      .transform(findPatientGender)
      .transform(findPatientDeathDate)
      .transform(fromDerivedPatienttoPatient)
      .distinct()
  }
}
