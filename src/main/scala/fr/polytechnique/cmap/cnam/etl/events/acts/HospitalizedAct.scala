package fr.polytechnique.cmap.cnam.etl.events.acts
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, EventCategory}

object HospitalizedAct extends MedicalActBuilder {
  override val category: EventCategory[MedicalActBuilder] = "hospitalized_act"
}
