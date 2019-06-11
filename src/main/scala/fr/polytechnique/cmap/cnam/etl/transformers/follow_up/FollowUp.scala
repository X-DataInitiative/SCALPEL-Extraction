package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.etl.events._

// case class Event[+A <: AnyEvent](
//     patientID: String,
//     category: EventCategory[A], // contains the category of the event ("diagnosis", "molecule", etc)
//     groupID: String, // contains the ID of a group of related events (e.g. hospitalization ID)
//     value: String, // contains the molecule name, the diagnosis code, etc.
//     weight: Double,
//     start: Timestamp,
//     end: Option[Timestamp]) {

trait FollowUp extends AnyEvent {
  val category: EventCategory[FollowUp]
  def apply(patientID: String, start: Timestamp, stop: Timestamp, endReason: String): Event[FollowUp] = {
    Event(patientID, category, "", endReason, 0.0, start, Some(stop))
  }

  def fromRow(
      r: Row,
      patientIDCol: String = "patientID",
      startCol: String = "start",
      stopCol: String = "end",
      endReasonCol: String = "endReason"
    ): Event[FollowUp] = {

    FollowUp(
      r.getAs[String](patientIDCol),
      r.getAs[Timestamp](startCol),
      r.getAs[Timestamp](stopCol),
      r.getAs[String](endReasonCol)
    )
  }
}

object FollowUp extends FollowUp {
  object Columns {
    final val PatientID = "patientID"
    final val Start = "start"
    final val End = "end"
    final val EndReason = "endReason"
  }
  override val category: EventCategory[FollowUp] = "follow_up"

}

