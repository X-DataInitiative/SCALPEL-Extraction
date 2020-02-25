package fr.polytechnique.cmap.cnam.study.bulk

import pureconfig.ConfigReader
import fr.polytechnique.cmap.cnam.etl.config.ConfigLoader
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.DrugClassConfig
import fr.polytechnique.cmap.cnam.study.fall.config.FallDrugClassConfig

class BulkConfigLoader extends ConfigLoader {

  //For reading DrugConfigClasses that are related to the Fall study
  implicit val drugConfigReader: ConfigReader[DrugClassConfig] = ConfigReader[String].map(
    family =>
      FallDrugClassConfig.familyFromString(family)
  )

}
