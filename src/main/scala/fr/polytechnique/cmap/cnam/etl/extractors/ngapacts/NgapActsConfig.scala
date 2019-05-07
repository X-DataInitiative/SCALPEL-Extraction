package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

class NgapActsConfig(val acts_categories: List[NgapActsClassConfig]) extends ExtractorConfig with Serializable

object NgapActsConfig {
  def apply(acts_categories: List[NgapActsClassConfig]): NgapActsConfig= new NgapActsConfig(
    acts_categories
  )
}