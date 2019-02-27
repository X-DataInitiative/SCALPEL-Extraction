package fr.polytechnique.cmap.cnam.etl.extractors.mco

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventBuilder}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait NewMcoExtractor[EventType <: AnyEvent] extends Extractor[EventType] with McoSource {

  def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RsaNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }

  def extractStart(r: Row): Timestamp = r.getAs[Timestamp](NewColumns.EstimatedStayStart)

  def extractWeight(r: Row): Double = 0.0

  def extractEnd(r: Row): Option[Timestamp] = None

  def getInput(sources: Sources): DataFrame = sources.mco.get.estimateStayStartTime

  val columnName: String
  val eventBuilder: EventBuilder

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

  lazy val code = (row: Row) => row.getAs[String](columnName)
}
