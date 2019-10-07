// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor

class MainDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = MainDiagnosis
}

object MainDiagnosisExtractor extends MainDiagnosisExtractor


class AssociatedDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = AssociatedDiagnosis
}

object AssociatedDiagnosisExtractor extends AssociatedDiagnosisExtractor


class LinkedDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = LinkedDiagnosis
}

object LinkedDiagnosisExtractor extends LinkedDiagnosisExtractor