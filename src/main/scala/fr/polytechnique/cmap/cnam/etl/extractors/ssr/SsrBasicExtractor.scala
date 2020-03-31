// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.ssr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.BasicExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait SsrBasicExtractor[EventType <: AnyEvent] extends BasicExtractor[EventType] with SsrRowExtractor {
  def getInput(sources: Sources): DataFrame = sources.ssr.get.estimateStayStartTime.select(neededColumns.map(col): _*)
}
