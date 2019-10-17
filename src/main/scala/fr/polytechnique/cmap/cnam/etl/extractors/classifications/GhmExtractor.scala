// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.classifications

import fr.polytechnique.cmap.cnam.etl.events.{Classification, EventBuilder, GHMClassification}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor

object GhmExtractor extends McoExtractor[Classification] {
  final override val columnName: String = ColNames.GHM
  override val eventBuilder: EventBuilder = GHMClassification
}
