// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.families

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Cardiac extends DrugClassConfig {
  override val name: String = "CardiacTherapy"
  override val cip13Codes: Set[String] = Set(
    "3400933489045",
    "3400930313374",
    "3400930313206",
    "3400930193945",
    "3400931163411",
    "3400932346554",
    "3400933466091",
    "3400930313893"
  )
  val cardiacGlycosides = new PharmacologicalClassConfig(
    name = "CardiacGlycosides",
    ATCCodes = List("C01AA*")
  )
  override val pharmacologicalClasses: List[PharmacologicalClassConfig] = List(cardiacGlycosides)

}
