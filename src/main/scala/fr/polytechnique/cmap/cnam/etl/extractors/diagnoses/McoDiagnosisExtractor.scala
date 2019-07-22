package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor

object MainDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DP
  override val category = MainDiagnosis.category
}


object AssociatedDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DA
  override val category = AssociatedDiagnosis.category
}


object LinkedDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DR
  override val category = LinkedDiagnosis.category
}
