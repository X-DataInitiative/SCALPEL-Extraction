package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.exceptions.WrongMatchException

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

  def merge(event: Event, patient: Patient): FlatEvent = {

    if(event.patientID != patient.patientID){
      throw WrongMatchException(event.patientID + " is different than " + patient.patientID)
    }
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
