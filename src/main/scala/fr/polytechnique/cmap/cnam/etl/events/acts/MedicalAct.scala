package fr.polytechnique.cmap.cnam.etl.events.acts

import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, EventCategory}

trait MedicalAct extends AnyEvent

object DcirCamAct extends MedicalActBuilder {
  val category: EventCategory[MedicalAct] = "dcir_act"
}

object McoCamAct extends MedicalActBuilder {
  val category: EventCategory[MedicalAct] = "mco_cam_act"
}

object McoCimAct extends MedicalActBuilder {
  val category: EventCategory[MedicalAct] = "mco_cim_act"
}