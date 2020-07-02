package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Vaccins extends DrugClassConfig {

  val name: String = "Vaccins"
  // Does it work to put empty cip13 codes ?
  val cip13Codes: Set[String] = Set()

  /*
  list pharmacological classes
   */

  val vaccinAntiGrippal= new PharmacologicalClassConfig(
    name = "Vaccin_Anti_Grippal",
    ATCCodes = List(
      "J07BB01",
      "J07BB02",
      "J07BB03"
    )
  )

  val pharmacologicalClasses = List(
    vaccinAntiGrippal
  )

}
