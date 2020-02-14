// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.dcir

import java.sql.Timestamp
import org.apache.commons.codec.binary.Base64
import scala.util.Try
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventBuilder}
import fr.polytechnique.cmap.cnam.etl.extractors.{EventRowExtractor, Extractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

trait DcirExtractor[EventType <: AnyEvent] extends Extractor[EventType] with DcirSource with EventRowExtractor {

  val columnName: String

  val eventBuilder: EventBuilder

  def getInput(sources: Sources): DataFrame = sources.dcir.get.select(ColNames.all.map(col): _*)

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

   /** Trying to catch unknown dates (example : IJ = Indemnité Journalière)
   *
   * @param r The Row object itself
   * @return The date of the event or the flux date if it doesn't exist
   */
  def extractStart(r: Row): Timestamp = {
    Try(r.getAs[java.util.Date](ColNames.Date).toTimestamp) recover {
      case _: NullPointerException => extractFluxDate(r)
    }
  }.get


  def extractFluxDate(r: Row): Timestamp = r.getAs[java.util.Date](ColNames.DcirFluxDate).toTimestamp

  /** Method to generate a hash value in a string format for the groupID value from a row with these values
    * FLX_DIS_DTD,FLX_TRT_DTD,FLX_EMT_TYP,FLX_EMT_NUM,FLX_EMT_ORD,ORG_CLE_NUM,DCT_ORD_NUM.
    * They are the 7 columns that identifies prescriptions in a unique way.
    *
    * @param r The Row object itself.
    * @return A hash Id unique in a string format.
    */
  override def extractGroupId(r: Row): String = {
    Base64.encodeBase64(
      s"${r.getAs[String](ColNames.DateStart)}_${r.getAs[String](ColNames.DateEntry)}_${
        r.getAs[String](
          ColNames
            .EmitterType
        )
      }_${r.getAs[String](ColNames.EmitterId)}_${r.getAs[String](ColNames.FlowNumber)}_${
        r.getAs[String](
          ColNames
            .OrgId
        )
      }_${r.getAs[String](ColNames.OrderId)}".getBytes()
    ).map(_.toChar).mkString

  }
}
