// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.mco

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.SimpleExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait McoSimpleExtractor[EventType <: AnyEvent] extends McoRowExtractor with SimpleExtractor[EventType]{
  def getInput(sources: Sources): DataFrame =
    sources.mco.get.select(neededColumns.map(col): _*).estimateStayStartTime
}
