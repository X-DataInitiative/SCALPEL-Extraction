// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.ssr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.SimpleExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait SsrSimpleExtractor[EventType <: AnyEvent] extends SsrRowExtractor with SimpleExtractor[EventType]{
  def getInput(sources: Sources): DataFrame = sources.ssr.get.estimateStayStartTime.select(neededColumns.map(col): _*)
}
