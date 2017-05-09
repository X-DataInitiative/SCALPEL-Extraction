package fr.polytechnique.cmap.cnam.etl.events.imb

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.EventRowExtractor

trait ImbEventRowExtractor extends EventRowExtractor with ImbSource {

  def getPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  def getGroupId(r: Row): String = "imb"

  def getCode(r: Row, colName: ColName, codes: List[String]): Option[String] = {
    val encoding = r.getAs[String](ColNames.Encoding)
    val idx = r.fieldIndex(colName)
    if (encoding != "CIM10" || r.isNullAt(idx))
      None
    else
      codes.find(r.getString(idx).startsWith(_))
  }

  def getWeight(r: Row): Double = 0.0

  def getStart(r: Row): Timestamp = r.getAs[Timestamp](ColNames.Date)

  def getEnd(r: Row): Option[Timestamp] = None: Option[Timestamp]
}
