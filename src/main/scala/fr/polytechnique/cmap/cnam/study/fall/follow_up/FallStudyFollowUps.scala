// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.follow_up

import java.sql.Timestamp
import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, FollowUp}
import fr.polytechnique.cmap.cnam.etl.patients.Patient

object FallStudyFollowUps {
  def transform(
    patients: Dataset[Patient],
    studyStart: Timestamp,
    studyEnd: Timestamp,
    startDelay: Int = 0): Dataset[(Patient, Event[FollowUp])] = {

    import patients.sparkSession.implicits._
    val startDate = (studyStart + startDelay.months).get
    patients.map { patient =>
      val endReason = if ((patient.deathDate.isDefined) && (patient.deathDate.get.before(studyEnd))) {
        "death"
      } else {
        "study_end"
      }
      val endDate: Timestamp = endReason match {
        case "death" => patient.deathDate.get
        case "study_end" => studyEnd
      }
      (patient, FollowUp(patient.patientID, endReason, startDate, endDate))
    }
  }
}
