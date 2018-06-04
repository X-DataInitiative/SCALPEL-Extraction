package fr.polytechnique.cmap.cnam.study.fall.config

import fr.polytechnique.cmap.cnam.etl.config.ConfigLoader
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugClassificationLevel, DrugConfig}
import fr.polytechnique.cmap.cnam.study.fall.BodySite
import pureconfig.ConfigReader

class FallConfigLoader extends ConfigLoader{

  //For reading DrugClassificationLevels
  implicit val drugClassificationLevelReader: ConfigReader[DrugClassificationLevel] = ConfigReader[String].map(level =>
    DrugClassificationLevel.fromString(level))

  //For reading DrugConfig
  implicit val drugConfigReader: ConfigReader[DrugConfig] = ConfigReader[String].map(family =>
    DrugConfig.familyFromString(family))

  //For reading Body Sites
  implicit val bodySitesReader: ConfigReader[BodySite] = ConfigReader[String].map( site =>
    BodySite.fromString(site))
}
