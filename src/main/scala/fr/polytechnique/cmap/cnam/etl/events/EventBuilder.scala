package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

trait EventBuilder { self: AnyEvent =>

  def apply(
    patientID: String, eventID: String, weight: Double, start: Timestamp, end: Option[Timestamp]
  ): Event[AnyEvent] =
    Event(patientID, this.category, eventID, weight, start, end)
}
