package fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mco.McoSimpleExtractor

protected trait SimpleMcoDiagnosisExtractor extends McoSimpleExtractor[Diagnosis] with StartsWithStrategy[Diagnosis]

case class McoMainDiagnosisExtractor(codes: SimpleExtractorCodes) extends SimpleMcoDiagnosisExtractor {
  override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = McoMainDiagnosis

  override def getCodes: SimpleExtractorCodes = codes
}

case class McoAssociatedDiagnosisExtractor(codes: SimpleExtractorCodes) extends SimpleMcoDiagnosisExtractor {
  override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = McoAssociatedDiagnosis

  override def getCodes: SimpleExtractorCodes = codes
}

case class McoLinkedDiagnosisExtractor(codes: SimpleExtractorCodes) extends SimpleMcoDiagnosisExtractor {
  override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = McoLinkedDiagnosis

  override def getCodes: SimpleExtractorCodes = codes
}
