package fr.polytechnique.cmap.cnam.etl.transformers.observation

import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.etl.events._

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
