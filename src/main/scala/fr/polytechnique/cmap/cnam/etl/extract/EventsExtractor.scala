package fr.polytechnique.cmap.cnam.etl.extract

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait EventsExtractor[A <: AnyEvent] {
  def extract(config: ExtractionConfig, sources: Sources): Dataset[Event[A]]
}
