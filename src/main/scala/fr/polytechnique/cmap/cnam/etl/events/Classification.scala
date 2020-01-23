// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row


trait Classification extends AnyEvent with EventBuilder {

  val category: EventCategory[Classification]

  def apply(
    patientID: String,
    groupID: String,
    name: String,
    date: Timestamp)
  : Event[Classification] = {
    Event(patientID, category, groupID, name, 0.0, date, None)
  }
}

object GHMClassification extends Classification {
  override val category: EventCategory[Classification] = "ghm"
}
