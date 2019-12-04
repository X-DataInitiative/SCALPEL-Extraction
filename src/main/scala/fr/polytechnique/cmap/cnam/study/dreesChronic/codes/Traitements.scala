package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Traitements extends DrugClassConfig {

  val name: String = "Traitements"
  val cip13Codes: Set[String] = Set()

  /*
  list pharmacological classes
   */

  val antiIgE= new PharmacologicalClassConfig(
    name = "Anti_IgE",
    ATCCodes = List("R03DX05")
  )

  val antiIL5= new PharmacologicalClassConfig(
    name = "Anti_Il5",
    ATCCodes = List(
      "R03DC03", //Antileucotriène
      "R03DX08", // Ac monoclonaux Traitement anti IL-5 
      "R03DX09")
  )

  val pharmacologicalClasses = List(
    antiIgE,
    antiIL5
  )

}
