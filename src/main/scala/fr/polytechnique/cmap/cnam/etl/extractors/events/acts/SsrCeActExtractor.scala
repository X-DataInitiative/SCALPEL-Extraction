package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, MedicalAct, SsrCEAct}
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.ssrce.SsrCeSimpleExtractor

final case class SsrCeActExtractor(codes: SimpleExtractorCodes) extends SsrCeSimpleExtractor[MedicalAct]
  with StartsWithStrategy[MedicalAct] {
  override def columnName: String = ColNames.CamCode

  override def eventBuilder: EventBuilder = SsrCEAct

  override def getCodes: SimpleExtractorCodes = codes
}
