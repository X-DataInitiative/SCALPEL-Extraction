// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

object Trackloss extends Trackloss

trait Trackloss extends AnyEvent with EventBuilder {

  val category: EventCategory[Trackloss] = "trackloss"

  def apply(patientID: String, timestamp: Timestamp): Event[Trackloss] = {
    Event(patientID, category, groupID = "NA", "trackloss", 0.0, timestamp, None)
  }
}
