package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

class DrugConfig(val level: DrugClassificationLevel, val families: List[DrugClassConfig]) extends ExtractorConfig with Serializable

object DrugConfig {
  def apply(level: DrugClassificationLevel, families: List[DrugClassConfig]): DrugConfig = new DrugConfig(
    level, families
  )
}