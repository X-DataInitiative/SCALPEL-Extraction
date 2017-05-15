package fr.polytechnique.cmap.cnam.etl.events.tracklosses

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventCategory}

trait Trackloss extends AnyEvent

object Trackloss extends Trackloss {

  val category: EventCategory[Trackloss] = "trackloss"

  def apply(patientID: String, timestamp: Timestamp): Event[Trackloss] = {
    Event(patientID, category, groupID = "NA", "trackloss", 0.0, timestamp, None)
  }

  def fromRow(
      r: Row,
      patientIDCol: String = "patientID",
      dateCol: String = "eventDate"): Event[Trackloss] = {

    Trackloss(r.getAs[String](patientIDCol), r.getAs[Timestamp](dateCol))
  }
}
