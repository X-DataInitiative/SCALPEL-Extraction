package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import java.sql.Timestamp

case class FollowUp(patientID: String, start: Timestamp, end: Timestamp, endReason: String) {
  def isValid: Boolean = start.before(end)
}

object FollowUp {
  object Columns {
    final val PatientID = "patientID"
    final val Start = "start"
    final val End = "end"
    final val EndReason = "endReason"
  }
}