// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.ExtractorCodes
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait Extractor[EventType <: AnyEvent, +Codes <: ExtractorCodes] extends Serializable {

  def getCodes: Codes

  /** Allows to check if the Row is considered in the current Study.
    *
    * @param row The row itself.
    * @return A boolean value.
    */
  def isInStudy(row: Row): Boolean


  /** Checks if the passed Row has the information needed to build the Event.
    *
    * @param row The row itself.
    * @return A boolean value.
    */
  def isInExtractorScope(row: Row): Boolean

  /** Builds the Event.
    *
    * @param row The row itself.
    * @return An event object.
    */
  def builder(row: Row): Seq[Event[EventType]]

  /** Gets and prepares all the needed columns from the Sources.
    *
    * @param sources Source object [[Sources]] that contains all sources.
    * @return A [[DataFrame]] with needed columns.
    */
  def getInput(sources: Sources): DataFrame

  /** Extracts the Event from the Source.
    *
    * This function is responsible for gluing different other parts of the Extractor.
    * This method should be considered the unique callable method from a Study perspective.
    *
    * @param sources Source object [[Sources]] that contains all sources.
    * @param ctag    An implicit parameter taken from EventType type.
    * @return A Dataset of Events of type EventType.
    */
  def extract(sources: Sources)(implicit ctag: TypeTag[EventType]): Dataset[Event[EventType]] = {
    val input: DataFrame = getInput(sources)

    import input.sqlContext.implicits._

    {
      if (getCodes.isEmpty) {
        input.filter(isInExtractorScope _)
      }
      else {
        input.filter(isInExtractorScope _).filter(isInStudy _)
      }
    }.flatMap(builder).distinct()
  }
}
