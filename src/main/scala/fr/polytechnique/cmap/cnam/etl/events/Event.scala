package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

// Check AnyEvent.scala for available event types
case class Event[+A <: AnyEvent](
    patientID: String,
    category: EventCategory[A], // contains the category of the event ("diagnosis", "molecule", etc)
    groupID: String, // contains the ID of a group of related events (e.g. hospitalization ID)
    value: String, // contains the molecule name, the diagnosis code, etc.
    weight: Double,
    start: Timestamp,
    end: Option[Timestamp]) {

  def checkValue(category: String, value: String): Boolean = {
    this.category == category && value == this.value
  }

  def checkValue(category: String, values: Seq[String]): Boolean = {
    this.category == category && values.contains(this.value)
  }
}

object Event {
  object Columns {
    val PatientID = "patientID"
    val Category = "category"
    val GroupID = "groupID"
    val Value = "value"
    val Weight = "weight"
    val Start = "start"
    val End = "end"
  }
}

