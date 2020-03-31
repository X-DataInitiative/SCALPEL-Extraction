// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.mco

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor

trait McoRowExtractor extends McoSource with EventRowExtractor {

  override def usedColumns: List[String] = ColNames.core ++ super.usedColumns

  /** It gets PatientID value from MCO source.
    *
    * @param r The row itself.
    * @return The value of PatientID.
    */
  def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  /** Creates an ID that group Events of different categories
    * by concatenating ETA_NUM, RSA_NUM and the YEAR.
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
}

