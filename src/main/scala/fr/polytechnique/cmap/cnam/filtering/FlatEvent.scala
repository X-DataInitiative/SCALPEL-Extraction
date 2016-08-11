package fr.polytechnique.cmap.cnam.filtering

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

object FlatEvent {

  def convert(event: Event, patient: Patient): FlatEvent = {
    FlatEvent(
      patientID = patient.patientID,
      gender = patient.gender,
      birthDate = patient.birthDate,
      deathDate = patient.deathDate,
      category = event.category,
      eventId = event.eventId,
      weight = event.weight,
      start = event.start,
      end = event.end
    )
  }
}
