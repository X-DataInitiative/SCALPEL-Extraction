package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.study.fall.codes.{Antidepresseurs, Antihypertenseurs, Hypnotiques, Neuroleptiques}


trait DrugConfig extends java.io.Serializable{

  val name: String
  val cip13Codes: Set[String]
  val pharmacologicalClasses: List[PharmacologicalClassConfig]

}

object DrugConfig{
  def familyFromString(family: String): DrugConfig = {
    family match {
      case "Antidepresseurs" => Antidepresseurs
      case "Antihypertenseurs" => Antihypertenseurs
      case "Neuroleptiques" => Neuroleptiques
      case "Hypnotiques" => Hypnotiques
    }
  }
}
