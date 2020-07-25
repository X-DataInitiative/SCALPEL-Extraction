// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.imb

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.SimpleExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait ImbSimpleExtractor[EventType <: AnyEvent] extends ImbRowExtractor with SimpleExtractor[EventType]{
  def getInput(sources: Sources): DataFrame = sources.irImb.get.select(neededColumns.map(col): _*)
}
