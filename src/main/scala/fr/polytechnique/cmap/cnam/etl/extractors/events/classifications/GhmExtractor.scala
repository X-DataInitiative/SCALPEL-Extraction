// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.classifications

import fr.polytechnique.cmap.cnam.etl.events.{Classification, EventBuilder, GHMClassification}
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mco.McoSimpleExtractor

final case class GhmExtractor(codes: SimpleExtractorCodes) extends McoSimpleExtractor[Classification]
  with StartsWithStrategy[Classification] {
  override val columnName: String = ColNames.GHM
  override val eventBuilder: EventBuilder = GHMClassification

  override def getCodes: SimpleExtractorCodes = codes
}