package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

/**
  * @author Daniel de Paula
  */
case class Event(
    patientID: String,
    category: String,
    eventId: String,
    weight: Double,
    start: Timestamp,
    end: Option[Timestamp])
