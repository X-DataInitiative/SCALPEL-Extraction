package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, MainDiagnosis, AssociatedDiagnosis, LinkedDiagnosis, EventBuilder}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.NewMcoExtractor

object NewMainDiagnosisExtractor extends NewMcoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = MainDiagnosis
}


object NewAssociatedDiagnosisExtractor extends NewMcoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = AssociatedDiagnosis
}


object NewLinkedDiagnosisExtractor extends NewMcoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = LinkedDiagnosis
}
