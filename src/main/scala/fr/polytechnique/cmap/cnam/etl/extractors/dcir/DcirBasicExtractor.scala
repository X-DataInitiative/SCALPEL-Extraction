// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.dcir

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.AnyEvent
import fr.polytechnique.cmap.cnam.etl.extractors.BasicExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait DcirBasicExtractor[EventType <: AnyEvent] extends BasicExtractor[EventType] with DcirRowExtractor {
  def getInput(sources: Sources): DataFrame = sources.dcir.get.select(neededColumns.map(col): _*)
}
