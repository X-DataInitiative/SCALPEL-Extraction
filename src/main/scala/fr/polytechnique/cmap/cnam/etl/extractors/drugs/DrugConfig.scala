package fr.polytechnique.cmap.cnam.etl.extractors.drugs

trait DrugConfig {

  val name: String
  val cip13Codes: List[String]
}
