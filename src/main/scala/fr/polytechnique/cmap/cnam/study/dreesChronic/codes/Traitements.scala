package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Traitements extends DrugClassConfig {

  val name: String = "Traitements"
  val cip13Codes: Set[String] = Set(
    "3400934612855",
    "3400934614118",
    "3400937022576",
    "3400939212265",
    "3400939212494",
    "3400939311241",
    "3400939312361"
  )

  /*
  list pharmacological classes
   */

  val antiIgE= new PharmacologicalClassConfig(
    name = "Anti_IgE",
    ATCCodes = List("R03DX05")
  )

  val antiIL5= new PharmacologicalClassConfig(
    name = "Anti_Il5",
    ATCCodes = List("R03DC03" ,"R03DX09", "R03DX09")
  )

  val pharmacologicalClasses = List(
    antiIgE,
    antiIL5
  )

}
