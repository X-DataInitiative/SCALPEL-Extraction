// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor

class McoMainDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = McoMainDiagnosis
}

object McoMainDiagnosisExtractor extends McoMainDiagnosisExtractor

class McoAssociatedDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = McoAssociatedDiagnosis
}

object McoAssociatedDiagnosisExtractor extends McoAssociatedDiagnosisExtractor

class McoLinkedDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = McoLinkedDiagnosis
}

object McoLinkedDiagnosisExtractor extends McoLinkedDiagnosisExtractor
