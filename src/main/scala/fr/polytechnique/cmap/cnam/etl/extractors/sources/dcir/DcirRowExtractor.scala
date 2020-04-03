// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.dcir

import java.sql.Timestamp
import scala.util.Try
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

/**
 * Gets the following fields for DCIR sourced events: patientID, start, groupId.
 */
trait DcirRowExtractor extends DcirSource with EventRowExtractor {

  override def usedColumns: List[ColName] = List(
    ColNames.PatientID, ColNames.DcirFluxDate, ColNames.DcirEventStart,
    ColNames.FlowDistributionDate, ColNames.FlowTreatementDate, ColNames.FlowEmitterType,
    ColNames.FlowEmitterId, ColNames.FlowEmitterNumber,
    ColNames.OrgId, ColNames.OrderId, ColNames.DcirEventStart
  ) ++ super.usedColumns

  def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  /** Trying to catch unknown dates
    * example of unknown dates situation : IJ = Indemnité Journalière which are a replacement income
    * paid by the HealthCare Insurance during a sick leave.
    *
    * @param r The Row object itself
    * @return The date of the event or the flux date if it doesn't exist
    */
  def extractStart(r: Row): Timestamp = {
    Try(r.getAs[java.util.Date](ColNames.DcirEventStart).toTimestamp) recover {
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
      s"${r.getAs[String](ColNames.FlowDistributionDate)}_${r.getAs[String](ColNames.FlowTreatementDate)}_${
        r.getAs[String](
          ColNames
            .FlowEmitterType
        )
      }_${r.getAs[String](ColNames.FlowEmitterId)}_${r.getAs[String](ColNames.FlowEmitterNumber)}_${
        r.getAs[String](
          ColNames
            .OrgId
        )
      }_${r.getAs[String](ColNames.OrderId)}".getBytes()
    ).map(_.toChar).mkString

  }
}

