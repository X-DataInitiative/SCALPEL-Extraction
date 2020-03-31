// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.classifications

import fr.polytechnique.cmap.cnam.etl.events.{Classification, EventBuilder, GHMClassification}
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, StartsWithStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoBasicExtractor

final case class GhmExtractor(codes: BaseExtractorCodes) extends McoBasicExtractor[Classification]
  with StartsWithStrategy[Classification] {
  override val columnName: String = ColNames.GHM
  override val eventBuilder: EventBuilder = GHMClassification

  override def getCodes: BaseExtractorCodes = codes
}