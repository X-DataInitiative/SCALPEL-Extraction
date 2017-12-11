package fr.polytechnique.cmap.cnam.etl.filters

import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import org.apache.spark.sql.Dataset

private[filters] class EventFiltersImplicits[T <: AnyEvent](events: Dataset[Event[T]]) {
  def filterPatients(filteredPatients: Dataset[Patient]): Dataset[Event[T]] = {
    import PatientFilters._
    val patientIds: Set[String] = filteredPatients.idsSet
    events.filter { e =>
      patientIds.contains(e.patientID)
    }
  }
}
