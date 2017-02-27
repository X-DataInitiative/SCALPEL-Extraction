package fr.polytechnique.cmap.cnam.filtering.extraction

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.filtering.Sources
import fr.polytechnique.cmap.cnam.filtering.events.{AnyEvent, Event}

trait EventsExtractor[T <: AnyEvent] {
  def extract(config: ExtractionConfig, sources: Sources): Dataset[Event[T]]
}
