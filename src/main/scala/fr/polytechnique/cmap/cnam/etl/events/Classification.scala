package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait Classification extends AnyEvent {

  val category: EventCategory[Classification]

  def fromRow(
    r: Row,
    patientIDCol: String = "patientID",
    nameCol: String = "name",
    groupIDCol: String = "groupID",
    dateCol: String = "eventDate")
  : Event[Classification] = {
    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](groupIDCol),
      r.getAs[String](nameCol),
      r.getAs[Timestamp](dateCol)
    )
  }

  def apply(
    patientID: String,
    groupID: String,
    name: String,
    date: Timestamp)
  : Event[Classification] = {
    Event(patientID, category, groupID, name, 0.0, date, None)
  }
}

object GHMClassification extends Classification {
  override val category: EventCategory[Classification] = "ghm"
}
