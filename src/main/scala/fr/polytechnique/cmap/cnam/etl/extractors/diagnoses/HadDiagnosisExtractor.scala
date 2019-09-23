package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.had.HadExtractor

object HadMainDiagnosisExtractor extends HadExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = HadMainDiagnosis
}

object HadAssociatedDiagnosisExtractor extends HadExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = HadAssociatedDiagnosis
}