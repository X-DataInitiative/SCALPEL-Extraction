package fr.polytechnique.cmap.cnam.etl.extractors.drugs

object DrugClassificationLevel extends Enumeration {
  type DrugClassificationLevel = Value
  val Therapeutic, Pharmacological, Molecule = Value
}
