package fr.polytechnique.cmap.cnam.study.fall.config

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugClassConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{Antidepresseurs, Antihypertenseurs, Hypnotiques, Neuroleptiques}


object FallDrugClassConfig {
  def familyFromString(family: String): DrugClassConfig = {
    family match {
      case "Antidepresseurs" => Antidepresseurs
      case "Antihypertenseurs" => Antihypertenseurs
      case "Neuroleptiques" => Neuroleptiques
      case "Hypnotiques" => Hypnotiques
    }
  }
}
