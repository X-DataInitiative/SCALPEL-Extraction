// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.had

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.SimpleExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait HadSimpleExtractor[EventType <: AnyEvent] extends HadRowExtractor with SimpleExtractor[EventType] {
  override def getInput(sources: Sources): DataFrame = sources.had.get.estimateStayStartTime
    .select(neededColumns.map(col): _*)

}
