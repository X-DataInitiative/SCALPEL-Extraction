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

  def checkValue(category: String, checkOp: (String) => Boolean): Boolean = {
    this.category == category && checkOp(this.value)
  }

  def checkValue(category: String, value: String): Boolean = {
    this.category == category && this.value == value
  }

  def checkValue(category: String, values: Seq[String]): Boolean = {
    this.category == category && values.contains(this.value)
  }

  def checkValue(value: String): Boolean = {
    this.value == value
  }

  def checkValue(values: Seq[String]): Boolean = {
    values.contains(this.value)
  }
}

