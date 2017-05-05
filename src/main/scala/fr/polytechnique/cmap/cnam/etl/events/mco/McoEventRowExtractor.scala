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
    r.getAs[Int](ColNames.Year).toString
  }

  def getCode(r: Row, colName: ColName, codes: List[String]): Option[String] = {
    val idx = r.fieldIndex(colName)
    codes.find(!r.isNullAt(idx) && r.getString(idx).startsWith(_))
  }

  def getWeight(r: Row): Double = 0.0

  def getStart(r: Row): Timestamp = r.getAs[Timestamp](NewColumns.EstimatedStayStart)

  def getEnd(r: Row): Option[Timestamp] = None: Option[Timestamp]
}
