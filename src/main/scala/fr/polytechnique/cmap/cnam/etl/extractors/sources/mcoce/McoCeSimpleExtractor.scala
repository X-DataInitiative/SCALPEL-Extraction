package fr.polytechnique.cmap.cnam.etl.extractors.sources.mcoce

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.SimpleExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait McoCeSimpleExtractor[EventType <: AnyEvent] extends McoCeRowExtractor with SimpleExtractor[EventType]{
  def getInput(sources: Sources): DataFrame = sources.mcoCe.get.select(neededColumns.map(col): _*)
}