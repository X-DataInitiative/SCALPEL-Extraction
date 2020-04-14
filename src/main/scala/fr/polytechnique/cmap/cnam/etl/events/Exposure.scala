// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

object Exposure extends Exposure

trait Exposure extends AnyEvent with EventBuilder {

  val category: EventCategory[Exposure] = "exposure"

  def apply(
    patientID: String, molecule: String, weight: Double, start: Timestamp, end: Timestamp
  ): Event[Exposure] = Event(patientID, category, groupID = "NA", molecule, weight, start, Some(end))
}
