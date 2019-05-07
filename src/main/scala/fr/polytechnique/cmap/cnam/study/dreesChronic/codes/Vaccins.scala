package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Vaccins extends DrugClassConfig {

  val name: String = "Vaccins"
  // Does it work to put empty cip13 codes ?
  val cip13Codes: Set[String] = Set(
    "3400933607852",
    "3400933652166",
    "3400935493828",
    "3400935947383",
    "3400939886091",
    "3400939886152",
    "3400939925745"
  )

  /*
  list pharmacological classes
   */

  val vaccinAntiGrippal= new PharmacologicalClassConfig(
    name = "Vaccin_Anti_Grippal",
    ATCCodes = List("J07BB02")
  )

  val pharmacologicalClasses = List(
    vaccinAntiGrippal
  )

}
