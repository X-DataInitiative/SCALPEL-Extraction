package fr.polytechnique.cmap.cnam.etl.extractors.drugs


trait DrugClassConfig extends java.io.Serializable {
  val name: String
  val cip13Codes: Set[String]
  val pharmacologicalClasses: List[PharmacologicalClassConfig]
}
