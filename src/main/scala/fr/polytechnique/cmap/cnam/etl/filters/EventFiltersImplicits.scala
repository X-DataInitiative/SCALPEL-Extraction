package fr.polytechnique.cmap.cnam.etl.filters

import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import org.apache.spark.sql.Dataset

private[filters] class EventFiltersImplicits[T <: AnyEvent](events: Dataset[Event[T]]) {
  def filterPatients(patientIds: Set[String]): Dataset[Event[T]] = {
    events.filter { e =>
      patientIds.contains(e.patientID)
    }
  }
}
