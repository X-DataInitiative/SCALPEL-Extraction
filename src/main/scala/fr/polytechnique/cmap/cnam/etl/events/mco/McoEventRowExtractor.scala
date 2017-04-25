package fr.polytechnique.cmap.cnam.etl.events.mco

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.EventRowExtractor

trait McoEventRowExtractor extends EventRowExtractor with McoSource {

  def getPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  def getEventCode(r: Row, colName: ColName, codes: List[String]): Option[String] = {
    codes.find(r.getAs[String](colName).startsWith(_))
  }

  def getEventId(r: Row): String = {
    r.getAs[String](ColNames.StayCode) + "_" + r.getAs[Int](ColNames.StayEndYear).toString
  }

  def getWeight(r: Row): Double = 1.0

  def getStart(r: Row): Timestamp = r.getAs[Timestamp](NewColumns.EstimatedStayStart)

  def getEnd(r: Row): Option[Timestamp] = None: Option[Timestamp]
}
