// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.mcoce

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

/**
 *  Gets the following fields for MCO_CE sourced events: patientID, start, groupId.
 */
trait McoCeRowExtractor extends McoCeSource with EventRowExtractor {
  override def usedColumns: List[String] = super.usedColumns ++ List(
    ColNames.PatientID, ColNames.EtaNum,
    ColNames.SeqNum, ColNames.Year,
    ColNames.StartDate
  )

  def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  /** Return groupID as hospital stay ID
    *
    * @param r
    * @return groupId which is the unique ID of the hospital stay
    */
  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.SeqNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }

  def extractStart(r: Row): Timestamp = r.getAs[Timestamp](ColNames.StartDate).toTimestamp

}
