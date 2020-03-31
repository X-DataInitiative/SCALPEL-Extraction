// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.extractors.{ExtractorCodes, ExtractorConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.DrugClassConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.DrugClassificationLevel

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