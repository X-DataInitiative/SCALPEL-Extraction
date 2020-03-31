package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, EventBuilder, HadAssociatedDiagnosis, HadMainDiagnosis}
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, StartsWithStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.had.HadBasicExtractor

final case class HadMainDiagnosisExtractor(codes: BaseExtractorCodes) extends HadBasicExtractor[Diagnosis] with
  StartsWithStrategy[Diagnosis] {
  override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = HadMainDiagnosis

  override def getCodes: BaseExtractorCodes = codes
}

final case class HadAssociatedDiagnosisExtractor(codes: BaseExtractorCodes) extends HadBasicExtractor[Diagnosis] with
  StartsWithStrategy[Diagnosis] {
  override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = HadAssociatedDiagnosis

  override def getCodes: BaseExtractorCodes = codes
}