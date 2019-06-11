package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

object Trackloss extends Trackloss

trait Trackloss extends AnyEvent {

  val category: EventCategory[Trackloss] = "trackloss"

  def fromRow(
    r: Row,
    patientIDCol: String = "patientID",
    dateCol: String = "eventDate"): Event[Trackloss] = {

    Trackloss(r.getAs[String](patientIDCol), r.getAs[Timestamp](dateCol))
  }

  def apply(patientID: String, timestamp: Timestamp): Event[Trackloss] = {
    Event(patientID, category, groupID = "NA", "trackloss", 0.0, timestamp, None)
  }
}
