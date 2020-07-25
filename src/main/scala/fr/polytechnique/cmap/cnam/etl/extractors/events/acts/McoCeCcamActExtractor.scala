// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.acts


import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, McoCeCcamAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mcoce.McoCeSimpleExtractor

final case class McoCeCcamActExtractor(codes: SimpleExtractorCodes) extends McoCeSimpleExtractor[MedicalAct]
  with StartsWithStrategy[MedicalAct] {
  override val eventBuilder: EventBuilder = McoCeCcamAct
  override val columnName: String = ColNames.CamCode

  override def getCodes: SimpleExtractorCodes = codes
}
