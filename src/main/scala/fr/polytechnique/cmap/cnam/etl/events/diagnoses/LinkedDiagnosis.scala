package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import fr.polytechnique.cmap.cnam.etl.events.EventCategory

object LinkedDiagnosis extends DiagnosisBuilder {
  val category: EventCategory[DiagnosisBuilder] = "linked_diagnosis"
}
