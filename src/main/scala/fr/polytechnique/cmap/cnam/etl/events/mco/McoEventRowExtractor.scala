package fr.polytechnique.cmap.cnam.etl.events.mco

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.EventRowExtractor

trait McoEventRowExtractor extends EventRowExtractor with McoSource {

  def getPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  def getGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
    r.getAs[String](ColNames.RsaNum) + "_" +
    r.getAs[Int](ColNames.StayEndYear).toString
  }

  def getCode(r: Row, colName: ColName, codes: List[String]): Option[String] = {
    codes.find(r.getAs[String](colName).startsWith(_))
  }

  def getWeight(r: Row): Double = 1.0

  def getStart(r: Row): Timestamp = r.getAs[Timestamp](NewColumns.EstimatedStayStart)

  def getEnd(r: Row): Option[Timestamp] = None: Option[Timestamp]
}
