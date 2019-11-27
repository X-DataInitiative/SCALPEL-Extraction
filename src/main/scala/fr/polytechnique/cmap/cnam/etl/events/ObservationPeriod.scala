package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

object ObservationPeriod extends ObservationPeriod

trait ObservationPeriod extends AnyEvent with EventBuilder {

  val category: EventCategory[ObservationPeriod] = "observation_period"

  def fromRow(
    r: Row,
    patientIDCol: String = "patientID",
    startCol: String = "start",
    endCol: String = "end"): Event[ObservationPeriod] = {

    ObservationPeriod(
      r.getAs[String](patientIDCol),
      r.getAs[Timestamp](startCol),
      r.getAs[Timestamp](endCol)
    )
  }


  def apply(patientID: String, start: Timestamp, end: Timestamp): Event[ObservationPeriod] =
    Event(patientID, category, groupID = "NA", value = "NA", weight = 0D, start, Some(end))
}
