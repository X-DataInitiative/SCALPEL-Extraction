// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.mco

import java.sql.Timestamp
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventBuilder}
import fr.polytechnique.cmap.cnam.etl.extractors.{EventRowExtractor, Extractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait McoExtractor[EventType <: AnyEvent] extends Extractor[EventType] with McoSource with EventRowExtractor {

  val columnName: String

  val eventBuilder: EventBuilder

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

  /** Extracts the tracked value.
   *
   * @return A string value.
   */
  def code = (row: Row) => row.getAs[String](columnName)

  /** It gets PatientID value from row.
   *
   * @param r The row itself.
   * @return The value of PatientID.
   */
  def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  /** Creates an ID that group Events of different categories
   * by concatinating ETA_NUM, RSA_NUM and the YEAR.
   *
   * @param r The row itself.
   * @return The value of groupId.
   */
  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RsaNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }

  /** Extracts the EstimatedStayStart as the start.
   * It comes from the method [[McoDataFrame.estimateStayStartTime]].
   *
   * @param r The row itself.
   * @return The value of EstimatedStayStart.
   */
  def extractStart(r: Row): Timestamp = r.getAs[Timestamp](NewColumns.EstimatedStayStart)

  /** It gets ExitMode from row.
   *
   * @param r The row itself.
   * @return The value of ExitMode.
   */
  def getExit(r: Row): String = r.getAs[String](ColNames.ExitMode)
}
