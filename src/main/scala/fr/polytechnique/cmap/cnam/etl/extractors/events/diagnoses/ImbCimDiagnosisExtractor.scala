// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses

import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, EventBuilder, ImbCcamDiagnosis}
import fr.polytechnique.cmap.cnam.etl.extractors.IsInStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.imb.ImbSimpleExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources


final case class ImbCimDiagnosisExtractor(codes: SimpleExtractorCodes) extends ImbSimpleExtractor[Diagnosis]
  with IsInStrategy[Diagnosis] {

  override def isInExtractorScope(row: Row): Boolean = {
    lazy val idx = row.fieldIndex(ColNames.Code)
    extractEncoding(row) == "CIM10" || !row.isNullAt(idx)
  }

  override def isInStudy(row: Row): Boolean = codes.exists(extractValue(row).startsWith(_))

  override def getInput(sources: Sources): DataFrame = sources.irImb.get

  override def columnName: String = ColNames.Code

  override def eventBuilder: EventBuilder = ImbCcamDiagnosis

  override def neededColumns: List[String] =
    List(ColNames.PatientID, ColNames.Date, ColNames.Encoding, ColNames.Code, ColNames.EndDate)

  override def getCodes: SimpleExtractorCodes = codes
}
