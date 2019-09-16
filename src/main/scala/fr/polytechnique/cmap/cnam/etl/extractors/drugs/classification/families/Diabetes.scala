package fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.families

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Diabetes extends DrugClassConfig {
  override val name: String = "Diabetes"
  override val cip13Codes: Set[String] = Set.empty[String]

  val diabetes = new PharmacologicalClassConfig(
    name = "Diabetes",
    ATCCodes = List("A10*"),
    ATCExceptions = List("A10BX06") //benfluorex
  )

  override val pharmacologicalClasses: List[PharmacologicalClassConfig] = List(diabetes)
}
