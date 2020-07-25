// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

/** Factory for FollowUp instances. */
object FollowUp extends FollowUp

/** This trait stores the methods required to create an Event object of type FollowUp. */
trait FollowUp extends AnyEvent with EventBuilder {

  val category: EventCategory[FollowUp] = "follow_up"

  /** Creates un Event object of type FollowUp using a map function to map a dataset.
   *
   * @param patientID The value patientID from dataset.
   * @param endReason The value endReason from dataset.
   * @param start     The value start from dataset.
   * @param end       The value end from dataset.
   * @return Event[FollowUp].
   */
  def apply(patientID: String, endReason: String, start: Timestamp, end: Timestamp): Event[FollowUp] =
    Event(patientID, category, groupID = "NA", endReason, weight = 0D, start, Some(end))
}