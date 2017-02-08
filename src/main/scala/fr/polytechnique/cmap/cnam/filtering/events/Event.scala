package fr.polytechnique.cmap.cnam.filtering.events

import java.sql.Timestamp

// Check AnyEvent.scala for available event types
case class Event[+T <: AnyEvent](
    patientID: String,
    category: EventCategory[T],
    eventID: String,
    weight: Double,
    start: Timestamp,
    end: Option[Timestamp])

