package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Antibiotiques extends DrugClassConfig {

  val name: String = "Antibiotiques"
  val cip13Codes: Set[String] = Set()

  /*
  list pharmacological classes
   */
  val atb= new PharmacologicalClassConfig(
    name = "Antibiotiques",
    ATCCodes = List(
      "J01CA04",
      "J01CR02",
      "J01FG01",
      "J01FA07",
      "J01FA02",
      "J01FA10",
      "J01DC02",
      "J01DD13",
      "J01DD04",
      "J01MA12",
      "J01FA15"
    )
  )


  val pharmacologicalClasses = List(
    atb
  )

}
