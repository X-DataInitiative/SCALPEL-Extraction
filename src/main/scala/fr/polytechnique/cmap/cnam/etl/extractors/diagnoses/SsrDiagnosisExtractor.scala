package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, EventBuilder, SsrAssociatedDiagnosis, SsrLinkedDiagnosis, SsrMainDiagnosis, SsrTakingOverPurpose}
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, StartsWithStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.ssr.SsrBasicExtractor

protected sealed abstract class SsrDiagnosisExtractor(codes: BaseExtractorCodes) extends SsrBasicExtractor[Diagnosis] with
  StartsWithStrategy[Diagnosis] {
  override def getCodes: BaseExtractorCodes = codes
}

final case class SsrMainDiagnosisExtractor(codes: BaseExtractorCodes) extends SsrDiagnosisExtractor(codes) {
  override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = SsrMainDiagnosis
}

final case class SsrAssociatedDiagnosisExtractor(codes: BaseExtractorCodes) extends SsrDiagnosisExtractor(codes) {
  override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = SsrAssociatedDiagnosis
}

final case class SsrLinkedDiagnosisExtractor(codes: BaseExtractorCodes) extends SsrDiagnosisExtractor(codes) {
  override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = SsrLinkedDiagnosis
}

final case class SsrTakingOverPurposeExtractor(codes: BaseExtractorCodes) extends SsrDiagnosisExtractor(codes) {
  override val columnName: String = ColNames.FP_PEC
  override val eventBuilder: EventBuilder = SsrTakingOverPurpose
}