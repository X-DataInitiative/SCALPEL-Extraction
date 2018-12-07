package fr.polytechnique.cmap.cnam.study.fall.config

import fr.polytechnique.cmap.cnam.etl.config.ConfigLoader
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugClassificationLevel, DrugClassConfig}
import fr.polytechnique.cmap.cnam.study.fall.fractures.BodySite
import pureconfig.ConfigReader

class FallConfigLoader extends ConfigLoader{

  //For reading DrugClassificationLevels
  implicit val drugClassificationLevelReader: ConfigReader[DrugClassificationLevel] = ConfigReader[String].map(level =>
    DrugClassificationLevel.fromString(level))

  //For reading DrugConfig
  implicit val drugConfigReader: ConfigReader[DrugClassConfig] = ConfigReader[String].map(family =>
    DrugClassConfig.familyFromString(family))

  //For reading Body Sites
  implicit val bodySitesReader: ConfigReader[BodySite] = ConfigReader[String].map( site =>
    BodySite.fromString(site))
}
