package fr.polytechnique.cmap.cnam.etl.extractors.acts


import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, McoCEAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.mcoCe.McoCeExtractor

object McoCeActExtractor extends McoCeExtractor[MedicalAct] {
  val columnName: String = ColNames.CamCode
  override val eventBuilder: EventBuilder = McoCEAct
}
