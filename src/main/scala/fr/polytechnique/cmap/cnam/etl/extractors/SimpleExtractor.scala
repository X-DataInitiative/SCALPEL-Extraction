// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventBuilder}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes

/**
 * Default Extractor implementation when the Extraction is simple.
 *
 * A simple Extractor is defined with the following characteristics:
 * 1. The passed codes are of the type [[SimpleExtractorCodes]].
 * 2. Every field of an [[Event]] is mapped simply by implementing [[EventRowExtractor]].
 * 3. The `inStudy` method of [[Extractor]] is implemented through [[InStudyStrategy]] implementation.
 *
 * This trait has self type of [[EventRowExtractor]]. Thus every implementation must be a type [[EventRowExtractor]].
 *
 * This trait defines two abstract methods:
 * 1. [[EventBuilder]]: Factory that produces [[Event]] of type [[EventType]].
 * 2. columnName: name of the column that produces the value field of the [[Event]].
 *
 * @tparam EventType Type of the [[Event]].
 */
trait SimpleExtractor[EventType <: AnyEvent] extends Extractor[EventType, SimpleExtractorCodes] {
  self: EventRowExtractor =>

  // Abstract methods of this trait
  def columnName: String

  def eventBuilder: EventBuilder

  // used in the getInput method to select the columns.
  def neededColumns: List[String] = columnName :: self.usedColumns

  // Unique method implementation of this trait of EventRowExtractor
  override def extractValue(row: Row): String = row.getAs[String](columnName)

  // Implementation of the Extractor trait
  def isInExtractorScope(row: Row): Boolean = !row.isNullAt(row.fieldIndex(columnName))

  def builder(row: Row): Seq[Event[EventType]] = {
    lazy val patientId = extractPatientId(row)
    lazy val groupId = extractGroupId(row)
    lazy val value = extractValue(row)
    lazy val eventDate = extractStart(row)
    lazy val endDate = extractEnd(row)
    lazy val weight = extractWeight(row)

    Seq(eventBuilder[EventType](patientId, groupId, value, weight, eventDate, endDate))
  }
}

/**
 * Defines the "inStudy" method of [[SimpleExtractor]].
 * @tparam EventType Type of Event to be extracted in the self typed [[SimpleExtractor]].
 */
sealed trait InStudyStrategy[EventType <: AnyEvent] {
  self: SimpleExtractor[EventType] =>
  override def isInStudy(row: Row): Boolean
}

trait AlwaysTrueStrategy[EventType <: AnyEvent] extends InStudyStrategy[EventType] {
  self: SimpleExtractor[EventType] =>
  def isInStudy(row: Row): Boolean = true
}

trait IsInStrategy[EventType <: AnyEvent] extends InStudyStrategy[EventType] {
  self: SimpleExtractor[EventType] =>
  def isInStudy(row: Row): Boolean = getCodes.contains(extractValue(row))
}

trait StartsWithStrategy[EventType <: AnyEvent] extends InStudyStrategy[EventType] {
  self: SimpleExtractor[EventType] =>
  def isInStudy(row: Row): Boolean = getCodes.exists(extractValue(row).startsWith)
}
