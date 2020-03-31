// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.acts


import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, McoCeCcamAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, StartsWithStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.mcoCe.McoCeBasicExtractor

final case class McoCeCcamActExtractor(codes: BaseExtractorCodes) extends McoCeBasicExtractor[MedicalAct]
  with StartsWithStrategy[MedicalAct] {
  override val eventBuilder: EventBuilder = McoCeCcamAct
  override val columnName: String = ColNames.CamCode

  override def getCodes: BaseExtractorCodes = codes
}
