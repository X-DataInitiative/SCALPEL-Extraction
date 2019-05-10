package fr.polytechnique.cmap.cnam.etl.filters

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}

object EventFilters {
  implicit def addEventImplicits[T <: AnyEvent](events: Dataset[Event[T]]): EventFiltersImplicits[T] = {
    new EventFiltersImplicits(events)
  }
}