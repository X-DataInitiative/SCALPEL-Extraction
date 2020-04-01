// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.drugs

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig
import fr.polytechnique.cmap.cnam.etl.extractors.codes.ExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.classification.DrugClassConfig
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.level.DrugClassificationLevel

class DrugConfig(
  val level: DrugClassificationLevel,
  val families: List[DrugClassConfig]) extends ExtractorConfig with ExtractorCodes {
  override def isEmpty: Boolean = families.isEmpty
}

object DrugConfig {
  def apply(level: DrugClassificationLevel, families: List[DrugClassConfig]): DrugConfig = new DrugConfig(
    level, families
  )
}