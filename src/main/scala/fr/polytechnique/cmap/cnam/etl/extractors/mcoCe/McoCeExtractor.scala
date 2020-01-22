package fr.polytechnique.cmap.cnam.etl.extractors.mcoCe

import java.sql.Timestamp
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventBuilder}
import fr.polytechnique.cmap.cnam.etl.extractors.{EventRowExtractor, Extractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.datetime.implicits._


trait McoCeExtractor[EventType <: AnyEvent] extends Extractor[EventType] with McoCeSource with EventRowExtractor {

  val columnName: String

  val eventBuilder: EventBuilder

  def getInput(sources: Sources): DataFrame = sources.mcoCe.get.select(ColNames.all.map(col): _*)

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

  def code = (row: Row) => row.getAs[String](columnName)

  def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.SeqNum) + "_" + r.getAs[Int](ColNames.Year).toString
  }

  def extractStart(r: Row): Timestamp = r.getAs[Timestamp](ColNames.Date).toTimestamp
}