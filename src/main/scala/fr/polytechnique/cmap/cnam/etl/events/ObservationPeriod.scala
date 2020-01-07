package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

/** Factory for ObservationPeriod instances. */
object ObservationPeriod extends ObservationPeriod

/** This trait stores the methods required to create an Event object of type ObservationPeriod. */
trait ObservationPeriod extends AnyEvent with EventBuilder {

  val category: EventCategory[ObservationPeriod] = "observation_period"

  /** Creates un Event object of type ObservationPeriod taken each row from a dataset.
   *
   * @param r            The Row object itself
   * @param patientIDCol The value patientID from dataset
   * @param startCol     The value start from dataset
   * @param endCol       The value end from dataset
   * @return Event[FollowUp]
   */
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

  /** Creates un Event object of type ObservationPeriod using a map function to map a dataset.
   *
   * @param patientID The value patientID from dataset
   * @param start     The value start from dataset
   * @param end       The value end from dataset
   * @return Event[ObservationPeriod]
   */
  def apply(patientID: String, start: Timestamp, end: Timestamp): Event[ObservationPeriod] =
    Event(patientID, category, groupID = "NA", value = "NA", weight = 0D, start, Some(end))
}
