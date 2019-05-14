package fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes

sealed trait CancerDefinition {
  // The following line is required to help the compiler to infer all subtypes of CancerDefinition
  // Important: the field type must not be explicit, otherwise the compiler won't do the inference.
  val types = List(CancerDefinition.Naive, CancerDefinition.Broad, CancerDefinition.Narrow)

  def toString: String
}

object CancerDefinition {

  val default: CancerDefinition = CancerDefinition.Naive

  case object Broad extends CancerDefinition

  case object Naive extends CancerDefinition

  case object Narrow extends CancerDefinition
}
