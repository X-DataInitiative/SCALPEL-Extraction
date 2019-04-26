package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, ImbDiagnosis}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.datetime

object ImbDiagnosisExtractor extends Extractor[Diagnosis] with ImbSource {

  override def isInExtractorScope(row: Row): Boolean = {
    lazy val idx = row.fieldIndex(ColNames.Code)
    getEncoding(row) == "CIM10" || !row.isNullAt(idx)
  }

  override def isInStudy(codes: Set[String])
    (row: Row): Boolean = codes.exists(getCode(row).startsWith(_))

  override def builder(row: Row): Seq[Event[Diagnosis]] =
    Seq(ImbDiagnosis(getPatientID(row), getCode(row), getEventDate(row)))

  override def getInput(sources: Sources): DataFrame = sources.irImb.get
}

trait ImbSource extends Serializable {

  lazy val getCode = (row: Row) => row.getAs[String](ColNames.Code)

  def getEncoding(row: Row): String = row.getAs[String](ColNames.Encoding)

  def getPatientID(row: Row): String = row.getAs[String](ColNames.PatientID)

  def getEventDate(row: Row): Timestamp = {
    import datetime.implicits._

    row.getAs[Date](ColNames.Date).toTimestamp
  }

  final object ColNames extends Serializable {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val Encoding = "MED_NCL_IDT"
    final lazy val Code = "MED_MTF_COD"
    final lazy val Date = "IMB_ALD_DTD"
  }
}