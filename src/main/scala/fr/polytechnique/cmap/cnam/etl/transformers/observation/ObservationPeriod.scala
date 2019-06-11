package fr.polytechnique.cmap.cnam.etl.transformers.observation

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

trait ObservationPeriod extends AnyEvent {
  val category: EventCategory[ObservationPeriod]
  def apply(patientID: String, start: Timestamp, stop: Timestamp): Event[ObservationPeriod] = {
    Event(patientID, category, "", "", 0.0, start, Some(stop))
  }

  def fromRow(
      r: Row,
      patientIDCol: String = "patientID",
      startCol: String = "start",
      stopCol: String = "stop"
    ): Event[ObservationPeriod] = {

    ObservationPeriod(
      r.getAs[String](patientIDCol),
      r.getAs[Timestamp](startCol),
      r.getAs[Timestamp](stopCol)
    )
  }
}

object ObservationPeriod extends ObservationPeriod {
  override val category: EventCategory[ObservationPeriod] = "observation_period"
}

// case class ObservationPeriod (patientID: String, start: Timestamp, stop: Timestamp)
