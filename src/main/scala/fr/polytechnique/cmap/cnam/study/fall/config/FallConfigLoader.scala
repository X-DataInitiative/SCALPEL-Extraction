package fr.polytechnique.cmap.cnam.study.fall.config

import pureconfig.ConfigReader
import fr.polytechnique.cmap.cnam.etl.config.ConfigLoader
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugClassConfig
import fr.polytechnique.cmap.cnam.study.fall.fractures.BodySite

class FallConfigLoader extends ConfigLoader {

  //For reading DrugConfigClasses that are related to the Fall study
  implicit val drugConfigReader: ConfigReader[DrugClassConfig] = ConfigReader[String].map(
    family =>
      FallDrugClassConfig.familyFromString(family)
  )

  //For reading Body Sites
  implicit val bodySitesReader: ConfigReader[BodySite] = ConfigReader[String].map(
    site =>
      BodySite.fromString(site)
  )
}
