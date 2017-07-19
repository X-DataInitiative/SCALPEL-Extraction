package fr.polytechnique.cmap.cnam.etl.transform.follow_up

import java.sql.Timestamp

case class FollowUp(patientID: String, start: Timestamp, stop: Timestamp, endReason: String)

object FollowUp {
  object Columns {
    final val PatientID = "patientID"
    final val Start = "start"
    final val Stop = "stop"
    final val EndReason = "endReason"
  }
}