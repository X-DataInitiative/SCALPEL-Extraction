package fr.polytechnique.cmap.cnam.etl.extractors.mco

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventBuilder}
import fr.polytechnique.cmap.cnam.etl.extractors.{EventRowExtractor, Extractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait McoExtractor[EventType <: AnyEvent] extends Extractor[EventType] with McoSource with EventRowExtractor {

  val columnName: String

  val eventBuilder: EventBuilder

  def code = (row: Row) => row.getAs[String](columnName)

  def getInput(sources: Sources): DataFrame = sources.mco.get.select(ColNames.all.map(col): _*).estimateStayStartTime

  def isInStudy(codes: Set[String])
    (row: Row): Boolean = codes.exists(code(row).startsWith(_))

  def isInExtractorScope(row: Row): Boolean = !row.isNullAt(row.fieldIndex(columnName))

  def builder(row: Row): Seq[Event[EventType]] = {
    lazy val patientId = extractPatientId(row)
    lazy val groupId = extractGroupId(row)
    lazy val eventDate = extractStart(row)
    lazy val endDate = extractEnd(row)
    lazy val weight = extractWeight(row)

    Seq(eventBuilder[EventType](patientId, groupId, code(row), weight, eventDate, endDate))
  }

  def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RsaNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }

  def extractStart(r: Row): Timestamp = r.getAs[Timestamp](NewColumns.EstimatedStayStart)
}
