package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import fr.polytechnique.cmap.cnam.etl.events.EventCategory

object AssociatedDiagnosis extends DiagnosisBuilder {
  val category: EventCategory[DiagnosisBuilder] = "associated_diagnosis"
}
