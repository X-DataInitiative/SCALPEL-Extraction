package fr.polytechnique.cmap.cnam.etl.extractors.classifications

import fr.polytechnique.cmap.cnam.etl.events.{Classification, EventBuilder, GHMClassification}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.NewMcoExtractor

object NewGhmExtractor extends NewMcoExtractor[Classification] {
  final override val columnName: String = ColNames.GHM
  override val eventBuilder: EventBuilder = GHMClassification
}
