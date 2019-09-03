package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

object Outcome extends Outcome

trait Outcome extends AnyEvent with EventBuilder {

  override val category: EventCategory[Outcome] = "outcome"

  def apply(patientID: String, groupId: String, name: String, date: Timestamp): Event[Outcome] =
    Event(patientID, category, groupID = groupId, name, 0.0, date, None)

  def apply(patientID: String, groupId: String, name: String, weight: Double, date: Timestamp): Event[Outcome] =
    Event(patientID, category, groupID = groupId, name, weight, date, None)

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

  def fromRow2(
               r: Row,
               patientIDCol: String = "patientID",
               nameCol: String = "name",
               weightCol: String = "weight",
               dateCol: String = "eventDate"): Event[Outcome] = {

    Outcome(
      r.getAs[String](patientIDCol),
      r.getAs[String](nameCol),
      r.getAs[Double](weightCol),
      r.getAs[Timestamp](dateCol)
    )
  }

  def apply(patientID: String, name: String, date: Timestamp): Event[Outcome] =
    Event(patientID, category, groupID = "NA", name, 0.0, date, None)

  def apply(patientID: String, name: String, weight: Double, date: Timestamp): Event[Outcome] =
    Event(patientID, category, groupID = "NA", name, weight, date, None)
}

