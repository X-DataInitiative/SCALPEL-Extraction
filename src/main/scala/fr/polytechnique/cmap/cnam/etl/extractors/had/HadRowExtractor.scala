package fr.polytechnique.cmap.cnam.etl.extractors.had

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor

trait HadRowExtractor extends HadSource with EventRowExtractor {

  override def usedColumns: List[String] = List(
    ColNames.PatientID, ColNames.EtaNumEpmsi, ColNames.RhadNum,
    NewColumns.Year, NewColumns.EstimatedStayStart, ColNames.StayStartDate
  ) ++ super.usedColumns

  def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNumEpmsi) + "_" +
      r.getAs[String](ColNames.RhadNum) + "_" +
      r.getAs[Int](NewColumns.Year).toString
  }

  def extractStart(r: Row): Timestamp = r.getAs[Timestamp](NewColumns.EstimatedStayStart)
}
