// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.imb

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.BasicExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait ImbBasicExtractor[EventType <: AnyEvent] extends BasicExtractor[EventType] with ImbRowExtractor {
  def getInput(sources: Sources): DataFrame = sources.irImb.get.select(neededColumns.map(col): _*)

  override def usedColumns: List[String] = super.usedColumns
}
