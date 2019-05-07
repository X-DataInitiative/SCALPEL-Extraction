package fr.polytechnique.cmap.cnam.study.dreesChronic.config

import pureconfig.ConfigReader
import fr.polytechnique.cmap.cnam.etl.config.ConfigLoader
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.DrugClassConfig
//import fr.polytechnique.cmap.cnam.study.dreesChronic.fractures.BodySite

class DreesChronicConfigLoader extends ConfigLoader {

  //For reading DrugConfigClasses that are related to the dreesChronic study
  implicit val drugConfigReader: ConfigReader[DrugClassConfig] = ConfigReader[String].map(
    family =>
      DreesChronicDrugClassConfig.familyFromString(family)
  )

  //For reading Body Sites
  /*implicit val bodySitesReader: ConfigReader[BodySite] = ConfigReader[String].map(
    site =>
      BodySite.fromString(site)
  )*/
}
