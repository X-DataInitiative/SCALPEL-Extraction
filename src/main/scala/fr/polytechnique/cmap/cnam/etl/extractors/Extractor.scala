// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors

import scala.reflect.runtime.universe._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait Extractor[EventType <: AnyEvent] extends Serializable {

  /**  Allows to check if the Row from the Source is considered in the current Study.
   *
   * @param codes A set of codes being considered in the Study.
   * @param row   The row itself.
   * @return A boolean value.
   */
  def isInStudy(codes: Set[String])(row: Row): Boolean


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

  /** Gets and prepares all the needed columns from the Source.
   *
   * @param sources Source object [[Sources]] that contains all sources.
   * @return A dataframe with mco columns.
   */
  def getInput(sources: Sources): DataFrame

  /** Extracts the Event from the Source.
   *
   * This function is responsible for gluing different other parts of the Extractor.
   * This method should be considered the unique callable method from a Study perspective.
   *
   * @param sources Source object [[Sources]] that contains all sources.
   * @param codes A set of codes passed through the method.
   * @param ctag An implicit parameter taken from Eventype type.
   * @return A dataset of Events.
   */
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