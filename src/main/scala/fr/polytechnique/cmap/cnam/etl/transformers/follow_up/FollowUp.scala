package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import java.sql.Timestamp

case class FollowUp(patientID: String, start: Timestamp, stop: Timestamp, endReason: String) {
  def isValid: Boolean = start.before(stop)
}

object FollowUp {
  object Columns {
    final val PatientID = "patientID"
    final val Start = "start"
    final val Stop = "stop"
    final val EndReason = "endReason"
  }
}