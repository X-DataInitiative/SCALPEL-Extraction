package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

trait MedicalTakeOverReason extends AnyEvent with EventBuilder {

  override val category: EventCategory[MedicalTakeOverReason]

  def apply(patientID: String, groupID: String, code: String, weight: Double, date: Timestamp): Event[MedicalTakeOverReason] = {
    Event(patientID, category, groupID, code, weight, date, None)
  }

  def apply(patientID: String, groupID: String, code: String, date: Timestamp): Event[MedicalTakeOverReason] = {
    Event(patientID, category, groupID, code, 0.0, date, None)
  }
}

object HadMainTakeOver extends MedicalTakeOverReason {
  val category: EventCategory[MedicalTakeOverReason] = "had_main_take_over_reason"
}

object HadAssociatedTakeOver extends MedicalTakeOverReason {
  val category: EventCategory[MedicalTakeOverReason] = "had_associated_take_over_reason"
}