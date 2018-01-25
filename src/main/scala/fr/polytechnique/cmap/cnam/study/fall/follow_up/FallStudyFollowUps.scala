package fr.polytechnique.cmap.cnam.study.fall.follow_up

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
import org.apache.spark.sql.Dataset

object FallStudyFollowUps {
  def transform(
      patients: Dataset[Patient],
      studyStart: Timestamp,
      studyEnd: Timestamp,
      startDelay: Int = 2): Dataset[(Patient, FollowUp)] = {

    import patients.sparkSession.implicits._
    patients.map { patient =>
      val endReason = if (patient.deathDate.isDefined) "death" else "study_end"
      (patient, FollowUp(patient.patientID, studyStart, patient.deathDate.getOrElse(studyEnd), endReason))
    }
  }
}
