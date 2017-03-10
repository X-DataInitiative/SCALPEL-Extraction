package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp

case class Event(
    patientID: String,
    category: String,
    eventId: String,
    weight: Double,
    start: Timestamp,
    end: Option[Timestamp])
