package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.DrugClassConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.DrugClassificationLevel

class DrugConfig(
  val level: DrugClassificationLevel,
  val families: List[DrugClassConfig]) extends ExtractorConfig with Serializable

object DrugConfig {
  def apply(level: DrugClassificationLevel, families: List[DrugClassConfig]): DrugConfig = new DrugConfig(
    level, families
  )
}
