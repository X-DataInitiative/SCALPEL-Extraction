// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.ssr.SsrSimpleExtractor

protected sealed abstract class SsrDiagnosisExtractor(codes: SimpleExtractorCodes) extends SsrSimpleExtractor[Diagnosis] with
  StartsWithStrategy[Diagnosis] {
  override def getCodes: SimpleExtractorCodes = codes
}

final case class SsrMainDiagnosisExtractor(codes: SimpleExtractorCodes) extends SsrDiagnosisExtractor(codes) {
  override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = SsrMainDiagnosis
}

final case class SsrAssociatedDiagnosisExtractor(codes: SimpleExtractorCodes) extends SsrDiagnosisExtractor(codes) {
  override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = SsrAssociatedDiagnosis
}

final case class SsrLinkedDiagnosisExtractor(codes: SimpleExtractorCodes) extends SsrDiagnosisExtractor(codes) {
  override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = SsrLinkedDiagnosis
}

final case class SsrTakingOverPurposeExtractor(codes: SimpleExtractorCodes) extends SsrDiagnosisExtractor(codes) {
  override val columnName: String = ColNames.FP_PEC
  override val eventBuilder: EventBuilder = SsrTakingOverPurpose
}