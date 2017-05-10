package fr.polytechnique.cmap.cnam.etl.events.imb

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.EventRowExtractor

trait ImbEventRowExtractor extends EventRowExtractor with ImbSource {

  override def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  def extractCode(r: Row, codes: List[String]): Option[String] = {
    val encoding = r.getAs[String](ColNames.Encoding)
    val idx = r.fieldIndex(ColNames.Code)
    if (encoding != "CIM10" || r.isNullAt(idx))
      None
    else
      codes.find(r.getString(idx).startsWith(_))
  }

  override def extractStart(r: Row): Timestamp = r.getAs[Timestamp](ColNames.Date)
}
