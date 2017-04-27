package fr.polytechnique.cmap.cnam.etl.old_root

import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event => NewEvent}
import java.sql.Timestamp

case class Event(
    patientID: String,
    category: String,
    eventId: String,
    weight: Double,
    start: Timestamp,
    end: Option[Timestamp])

object Event {

  def fromNewEvent(e: NewEvent[AnyEvent]) = {
    Event(e.patientID, e.category, e.value, e.weight, e.start, e.end)
  }
}
