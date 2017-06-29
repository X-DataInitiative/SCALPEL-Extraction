package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

trait EventBuilder extends Serializable { self: AnyEvent =>

  def apply[T <: AnyEvent](
      patientID: String,
      // No category parameter. It's taken from the "category" attribute of the AnyEvent trait.
      groupID: String,
      value: String,
      weight: Double,
      start: Timestamp,
      end: Option[Timestamp]): Event[T] = {

    Event[T](patientID, this.category, groupID, value, weight, start, end)
  }
}
