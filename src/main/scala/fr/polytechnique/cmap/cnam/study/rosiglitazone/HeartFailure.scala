package fr.polytechnique.cmap.cnam.study.rosiglitazone

import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer

object HeartFailure extends OutcomeTransformer{

  override val outcomeName: String = "heart_failure"

}
