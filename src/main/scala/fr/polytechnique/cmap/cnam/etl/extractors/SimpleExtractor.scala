// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventBuilder}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes

trait SimpleExtractor[EventType <: AnyEvent] extends Extractor[EventType, SimpleExtractorCodes] {
  self: EventRowExtractor =>

  def columnName: String
  def eventBuilder: EventBuilder
  def neededColumns: List[String] = columnName :: self.usedColumns

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

  override def extractValue(row: Row): String = row.getAs[String](columnName)
}

sealed trait InStudyStrategy[EventType <: AnyEvent] {
  self: SimpleExtractor[EventType]=>
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
  def isInStudy(row: Row): Boolean =  getCodes.exists(extractValue(row).startsWith)
}
