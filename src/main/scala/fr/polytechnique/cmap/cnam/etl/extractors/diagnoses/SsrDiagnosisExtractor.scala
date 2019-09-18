package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.ssr.SsrExtractor

object SsrMainDiagnosisExtractor extends SsrExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = SsrMainDiagnosis
}


object SsrAssociatedDiagnosisExtractor extends SsrExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = SsrAssociatedDiagnosis
}


object SsrLinkedDiagnosisExtractor extends SsrExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = SsrLinkedDiagnosis
}
