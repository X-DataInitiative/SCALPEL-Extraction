package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, MainDiagnosis, AssociatedDiagnosis, LinkedDiagnosis, EventBuilder}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor

object MainDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = MainDiagnosis
}


object AssociatedDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = AssociatedDiagnosis
}


object LinkedDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = LinkedDiagnosis
}
