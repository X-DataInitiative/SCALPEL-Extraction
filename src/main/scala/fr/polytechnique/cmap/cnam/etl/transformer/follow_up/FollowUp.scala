package fr.polytechnique.cmap.cnam.etl.transformer.follow_up

import java.sql.Timestamp

case class FollowUp(patientID: String, start: Timestamp, stop: Timestamp)