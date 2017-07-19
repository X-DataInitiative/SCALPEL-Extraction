package fr.polytechnique.cmap.cnam.etl.extractors.mco

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor

trait McoEventRowExtractor extends EventRowExtractor with McoSource {

  override def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
    r.getAs[String](ColNames.RsaNum) + "_" +
    r.getAs[Int](ColNames.Year).toString
  }

  override def extractStart(r: Row): Timestamp = r.getAs[Timestamp](NewColumns.EstimatedStayStart)

  def extractCode(r: Row, colName: ColName, codes: Seq[String]): Option[String] = {
    val idx = r.fieldIndex(colName)
    codes.find(!r.isNullAt(idx) && r.getString(idx).startsWith(_))
  }

  def eventFromRow[A <: AnyEvent](
      r: Row, builder: EventBuilder, colName: ColName, codes: Seq[String]): Option[Event[A]] = {

    val patientId: String = extractPatientId(r)
    val foundCode: Option[String] = extractCode(r, colName, codes)
    val groupId: String = extractGroupId(r)
    val eventDate: Timestamp = extractStart(r)

    foundCode match {
      case None => None
      case Some(code) => Some(
        builder[A](patientId, groupId, code, extractWeight(r), eventDate, extractEnd(r)))
    }
  }
}
