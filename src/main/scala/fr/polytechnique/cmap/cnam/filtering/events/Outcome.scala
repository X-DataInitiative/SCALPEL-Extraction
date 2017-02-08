package fr.polytechnique.cmap.cnam.filtering.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait Outcome extends AnyEvent

object Outcome extends Outcome {

  val category: EventCategory[Outcome] = "outcome"

  def apply(patientID: String, name: String, date: Timestamp): Event[Outcome] =
    Event(patientID, category, name, 0.0, date, None)

  def fromRow(
      r: Row,
      patientIDCol: String = "patientID",
      nameCol: String = "name",
      dateCol: String = "eventDate"): Event[Outcome] = {

    Outcome(
      r.getAs[String](patientIDCol),
      r.getAs[String](nameCol),
      r.getAs[Timestamp](dateCol)
    )
  }
}
