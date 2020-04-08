package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HadCCAMAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.had.HadSimpleExtractor

final case class HadCcamActExtractor(codes: SimpleExtractorCodes) extends HadSimpleExtractor[MedicalAct]
  with StartsWithStrategy[MedicalAct] {
  override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = HadCCAMAct
  override def getCodes: SimpleExtractorCodes = codes
}


