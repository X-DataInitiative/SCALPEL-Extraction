package fr.polytechnique.cmap.cnam.etl.filters

import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import org.apache.spark.sql.Dataset

object EventFilters {
  implicit def addEventImplicits[T <: AnyEvent](events: Dataset[Event[T]]): EventFiltersImplicits[T] = {
    new EventFiltersImplicits(events)
  }
}