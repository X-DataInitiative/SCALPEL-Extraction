package fr.polytechnique.cmap.cnam.etl.transformers.patients

import java.sql.Timestamp
import java.time.Period
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.extractors.patients.PatientsConfig
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class PatientFilters(config: PatientsConfig) extends Serializable {

  /** Filter Patient with config
   *
   * @param patients the patient to check.
   * @return [[Dataset]] of Patient
   */
  def filterPatients(patients: Dataset[Patient]): Dataset[Patient] = {
    import patients.sparkSession.implicits._
    patients
      .flatMap(controlAge(config.minAge, config.maxAge))
      .flatMap(controlGender(config.minGender, config.maxGender))
      .flatMap(controlDeathDate(config.minYear, config.maxYear))
  }

  /** Returns None if Patient is old before minAge or after maxAge, otherwise returns Some(Patient)
   *
   * @param patient the patient to check.
   * @param minAge  the minimum year to control with.
   * @param maxAge  the maximum year to control with.
   * @return an Option of Patient
   */
  protected[patients] def controlAge(minAge: Int, maxAge: Int)(patient: Patient): TraversableOnce[Patient] = {
    val ageReferenceDate: Timestamp = Timestamp.valueOf(config.ageReferenceDate.atStartOfDay())
    if(patient.birthDate != null) {
      val age = Period.between(patient.birthDate.toLocalDateTime.toLocalDate, ageReferenceDate.toLocalDateTime.toLocalDate).getYears
      if (age >= config.minAge && age < config.maxAge) {
        Some(patient)
      } else {
        None
      }
    }
    else {
      None
    }
  }

  /** Returns None if Patient is died before minYear or after maxYear, otherwise returns Some(Patient)
   *
   * @param patient the patient to check.
   * @param minYear the minimum year to control with.
   * @param maxYear the maximum year to control with.
   * @return an Option of Patient
   */
  protected[patients] def controlDeathDate(minYear: Int, maxYear: Int)(patient: Patient): TraversableOnce[Patient] = {
    if (patient.deathDate.isEmpty || (patient.deathDate.get.after(makeTS(minYear, 1, 1)) && patient.deathDate.get.before(makeTS(maxYear, 1, 1)))) {
      Some(patient)
    } else {
      None
    }
  }

  /** Returns None if Patient has a unknown Gender, otherwise returns Some(Patient)
   *
   * @param patient   the patient to check.
   * @param minGender the minimum gender to control with.
   * @param maxGender the maximum gender to control with.
   * @return an Option of Patient
   */
  protected[patients] def controlGender(minGender: Int, maxGender: Int)(patient: Patient): TraversableOnce[Patient] = {
    if (patient.gender >= minGender && patient.gender <= maxGender) {
      Some(patient)
    } else {
      None
    }
  }
}
