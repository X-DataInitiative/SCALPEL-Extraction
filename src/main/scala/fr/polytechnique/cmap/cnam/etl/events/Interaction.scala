// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

object Interaction extends Interaction {
  def apply(exposure: Event[Exposure]): Event[Interaction] = Event(
    exposure.patientID,
    category,
    groupID = exposure.groupID,
    exposure.value,
    1.0D,
    exposure.start,
    exposure.end
  )

  def apply(
    patientID: String, value: String, weight: Double, start: Timestamp, end: Timestamp
  ): Event[Interaction] = Event(patientID, category, groupID = "NA", value, weight, start, Some(end))
}

trait Interaction extends AnyEvent with EventBuilder {
  val category: EventCategory[Interaction] = "interaction"
}
