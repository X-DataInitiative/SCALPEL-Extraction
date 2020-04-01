package fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, EventBuilder, HadAssociatedDiagnosis, HadMainDiagnosis}
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.had.HadSimpleExtractor

final case class HadMainDiagnosisExtractor(codes: SimpleExtractorCodes) extends HadSimpleExtractor[Diagnosis] with
  StartsWithStrategy[Diagnosis] {
  override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = HadMainDiagnosis

  override def getCodes: SimpleExtractorCodes = codes
}

final case class HadAssociatedDiagnosisExtractor(codes: SimpleExtractorCodes) extends HadSimpleExtractor[Diagnosis] with
  StartsWithStrategy[Diagnosis] {
  override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = HadAssociatedDiagnosis

  override def getCodes: SimpleExtractorCodes = codes
}