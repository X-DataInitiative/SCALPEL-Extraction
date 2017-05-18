package fr.polytechnique.cmap.cnam.etl.transformer.observation

import java.sql.Timestamp

case class ObservationPeriod (patientID: String, start: Timestamp, stop: Timestamp)
