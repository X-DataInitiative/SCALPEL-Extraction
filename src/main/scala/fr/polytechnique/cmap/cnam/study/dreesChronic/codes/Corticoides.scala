package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Corticoides extends DrugClassConfig {

  val name: String = "Corticoides"
  val cip13Codes: Set[String] = Set()

  /*
  list pharmacological classes
   */
  val csi= new PharmacologicalClassConfig(
    name = "Corticoides",
    ATCCodes = List(
        "R03BA01", // beclometasone
        "R03BA02", //budesonide
        "R03BA09", // fluticasone fuoate (flixoide, non présente dans les bases...)
        "R03BA05" // fluticasone
    )
  )


  val pharmacologicalClasses = List(
    csi
  )

}
