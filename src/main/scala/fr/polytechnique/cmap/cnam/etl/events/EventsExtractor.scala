package fr.polytechnique.cmap.cnam.etl.events

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait EventsExtractor[T <: AnyEvent] {
  def extract(config: ExtractionConfig, sources: Sources): Dataset[Event[T]]
}
