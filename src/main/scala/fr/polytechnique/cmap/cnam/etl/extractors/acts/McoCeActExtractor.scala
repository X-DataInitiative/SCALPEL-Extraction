package fr.polytechnique.cmap.cnam.etl.extractors.acts

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{Event, McoCEAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

object McoCeActExtractor extends Extractor[MedicalAct] with McoCeSourceExtractor {
  override def isInStudy(codes: Set[String])
    (row: Row): Boolean = codes.exists(getCode(row).startsWith(_))

  override def isInExtractorScope(row: Row): Boolean = !isNullAt(ColNames.CamCode)(row)

  override def builder(row: Row): Seq[Event[MedicalAct]] = {
    lazy val patientID = getPatientID(row)
    lazy val date = getDate(row)
    lazy val code = getCode(row)

    Seq(McoCEAct(patientID, "ACE", code, date))
  }

  override def getInput(sources: Sources): DataFrame = sources.mcoCe.get
}


trait McoCeSourceExtractor {

  def getPatientID(row: Row): String = row.getAs[String](ColNames.PatientID)

  def getDate(row: Row): Timestamp = row.getAs[Date](ColNames.Date).toTimestamp

  def getCode(row: Row): String = row.getAs[String](ColNames.CamCode)

  def isNullAt(colName: String)(row: Row): Boolean = row.isNullAt(row.fieldIndex(colName))

  final object ColNames extends Serializable {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val CamCode = "MCO_FMSTC__CCAM_COD"
    final lazy val Date = "EXE_SOI_DTD"
  }

}
