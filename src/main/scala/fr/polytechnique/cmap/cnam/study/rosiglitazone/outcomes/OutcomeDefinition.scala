package fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes

sealed trait OutcomeDefinition {
  // The following line is required to help the compiler to infer all subtypes of OutcomeDefinition
  // Important: the field type must not be explicit, otherwise the compiler won't do the inference.
  val types = List(OutcomeDefinition.Infarctus, OutcomeDefinition.HeartFailure)

  val outcomeName: String
}

object OutcomeDefinition {
  case object Infarctus extends OutcomeDefinition {
    val outcomeName = "infarctus"
  }
  case object HeartFailure extends OutcomeDefinition {
    val outcomeName = "heart_failure"
  }
  val default: OutcomeDefinition = OutcomeDefinition.Infarctus
}
