package fr.polytechnique.cmap.cnam.study.dreesChronic.config

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification._
import fr.polytechnique.cmap.cnam.study.dreesChronic.codes._

object DreesChronicDrugClassConfig {
  def familyFromString(family: String): DrugClassConfig = {
    family match {
      case "Antibiotiques" => Antibiotiques
      case "Bronchodilatateurs" => Bronchodilatateurs
      case "Nicotiniques" => Nicotiniques
      case "Traitements" => Traitements
      case "Vaccins" => Vaccins
    }
  }
}
