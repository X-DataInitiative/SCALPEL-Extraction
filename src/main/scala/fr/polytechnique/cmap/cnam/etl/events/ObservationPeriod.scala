package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

/** Factory for ObservationPeriod instances. */
object ObservationPeriod extends ObservationPeriod

/** This trait stores the methods required to create an Event object of type ObservationPeriod. */
trait ObservationPeriod extends AnyEvent with EventBuilder {

  val category: EventCategory[ObservationPeriod] = "observation_period"

  /** Creates un Event object of type ObservationPeriod using a map function to map a dataset.
   *
   * @param patientID The value patientID from dataset.
   * @param start     The value start from dataset.
   * @param end       The value end from dataset.
   * @return Event[ObservationPeriod].
   */
  def apply(patientID: String, start: Timestamp, end: Timestamp): Event[ObservationPeriod] =
    Event(patientID, category, groupID = "NA", value = "NA", weight = 0D, start, Some(end))
}