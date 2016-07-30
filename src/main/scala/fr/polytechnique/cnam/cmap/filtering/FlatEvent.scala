package fr.polytechnique.cnam.cmap.filtering

import java.sql.Timestamp

/**
  * @author Daniel de Paula
  */
case class FlatEvent(
    patientID: String,
    gender: Int,
    birthDate: Timestamp,
    deathDate: Option[Timestamp],
    category:String,
    eventId: String,
    weight: Double,
    start: Timestamp,
    end: Option[Timestamp])
