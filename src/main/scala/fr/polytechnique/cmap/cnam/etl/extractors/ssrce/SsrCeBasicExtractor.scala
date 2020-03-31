// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.ssrce

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.BasicExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait SsrCeBasicExtractor [EventType <: AnyEvent] extends BasicExtractor[EventType] with SsrCeRowExtractor {
  def getInput(sources: Sources): DataFrame = sources.ssrCe.get.select(neededColumns.map(col): _*)

}
