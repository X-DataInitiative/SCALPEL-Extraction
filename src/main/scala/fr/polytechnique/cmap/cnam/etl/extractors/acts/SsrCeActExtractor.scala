package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, MedicalAct, SsrCEAct}
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, StartsWithStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.ssrce.SsrCeBasicExtractor

final case class SsrCeActExtractor(codes: BaseExtractorCodes) extends SsrCeBasicExtractor[MedicalAct]
  with StartsWithStrategy[MedicalAct] {
  override def columnName: String = ColNames.CamCode

  override def eventBuilder: EventBuilder = SsrCEAct

  override def getCodes: BaseExtractorCodes = codes
}
