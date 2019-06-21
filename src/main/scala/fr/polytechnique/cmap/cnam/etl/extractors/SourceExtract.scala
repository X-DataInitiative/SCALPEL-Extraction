package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Timestamp
import org.apache.spark.sql._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources

abstract class SourceExtractor extends Serializable {

  def select() : DataFrame
  def extract[A <: AnyEvent]() : Dataset[Event[A]]
}

