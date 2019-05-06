package fr.polytechnique.cmap.cnam.etl.extractors

import scala.reflect.runtime.universe._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait Extractor[EventType <: AnyEvent] extends Serializable {

  def isInStudy(codes: Set[String])(row: Row): Boolean

  def isInExtractorScope(row: Row): Boolean

  def builder(row: Row): Seq[Event[EventType]]

  def getInput(sources: Sources): DataFrame

  def extract(sources: Sources, codes: Set[String])(implicit ctag: TypeTag[EventType]): Dataset[Event[EventType]] = {
    val input: DataFrame = getInput(sources)

    import input.sqlContext.implicits._

    {
      if (codes.isEmpty) {
        input.filter(isInExtractorScope _)
      }
      else {
        input.filter(isInExtractorScope _).filter(isInStudy(codes) _)
      }
    }.flatMap(builder _).distinct()
  }

}