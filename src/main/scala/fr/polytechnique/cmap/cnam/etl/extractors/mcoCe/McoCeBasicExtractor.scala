package fr.polytechnique.cmap.cnam.etl.extractors.mcoCe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.BasicExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait McoCeBasicExtractor[EventType <: AnyEvent] extends BasicExtractor[EventType] with McoCeRowExtractor {
  def getInput(sources: Sources): DataFrame = sources.mcoCe.get.select(neededColumns.map(col): _*)
}