package fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes

import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer

object HeartFailure extends OutcomesTransformer {

  override val outcomeName: String = OutcomeDefinition.HeartFailure.outcomeName
}
