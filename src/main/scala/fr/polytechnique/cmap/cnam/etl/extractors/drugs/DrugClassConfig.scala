package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import java.io.Serializable


trait DrugClassConfig extends Serializable {
  val name: String
  val cip13Codes: Set[String]
  val pharmacologicalClasses: List[PharmacologicalClassConfig]
}
