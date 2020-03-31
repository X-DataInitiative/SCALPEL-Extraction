package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HadCCAMAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, StartsWithStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.had.HadBasicExtractor

final case class HadCcamActExtractor(codes: BaseExtractorCodes) extends HadBasicExtractor[MedicalAct]
  with StartsWithStrategy[MedicalAct] {
  override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = HadCCAMAct
  override def getCodes: BaseExtractorCodes = codes
}


