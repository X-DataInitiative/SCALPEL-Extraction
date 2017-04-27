package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

// Check AnyEvent.scala for available event types
case class Event[+T <: AnyEvent](
    patientID: String,
    category: EventCategory[T], // contains the category of the event ("diagnosis", "molecule", etc)
    groupID: String, // contains the ID of a group of related events (e.g. hospitalization ID)
    value: String, // contains the molecule name, the diagnosis code, etc.
    weight: Double,
    start: Timestamp,
    end: Option[Timestamp])

