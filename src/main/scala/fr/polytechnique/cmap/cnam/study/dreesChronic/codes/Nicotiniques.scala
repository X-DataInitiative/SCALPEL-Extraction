package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Nicotiniques extends DrugClassConfig {

  val name: String = "Nicotiniques"
  val cip13Codes: Set[String] = Set()

  /*
  list pharmacological classes
   */

  val subNicotiniques= new PharmacologicalClassConfig(
    name = "Sub_Nicotiniques",
    ATCCodes = List("N07BA01", "N07BA03", "N06AX12")
  )

  val pharmacologicalClasses = List(
    subNicotiniques
  )

}
