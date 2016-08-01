package fr.polytechnique.cnam.cmap.filtering

import java.sql.Timestamp

/**
  * @author Daniel de Paula
  */
case class Event(
    patient: Patient,
    category: String,
    eventId: String,
    weight: Double,
    start: Timestamp,
    end: Option[Timestamp])
