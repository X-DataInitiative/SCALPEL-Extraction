package fr.polytechnique.cmap.cnam.etl.events.acts
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, EventCategory}

object NonHospitalizedAct extends MedicalActBuilder {
  override val category: EventCategory[MedicalActBuilder] = "non_hospitalized_act"
}
