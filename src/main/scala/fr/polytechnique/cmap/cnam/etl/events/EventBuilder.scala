package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

trait EventBuilder { self: AnyEvent =>

  def apply(
    patientID: String, groupID: String, value: String, weight: Double, start: Timestamp, end: Option[Timestamp]
  ): Event[AnyEvent] =
    Event(patientID, this.category, groupID, value, weight, start, end)
}
