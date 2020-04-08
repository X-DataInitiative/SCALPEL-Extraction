// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.ssrce

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.SimpleExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait SsrCeSimpleExtractor [EventType <: AnyEvent] extends SsrCeRowExtractor with SimpleExtractor[EventType]{
  def getInput(sources: Sources): DataFrame = sources.ssrCe.get.select(neededColumns.map(col): _*)
}
