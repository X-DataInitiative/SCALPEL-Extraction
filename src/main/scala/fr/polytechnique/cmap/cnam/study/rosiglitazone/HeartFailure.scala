package fr.polytechnique.cmap.cnam.study.rosiglitazone

import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer

object HeartFailure extends OutcomesTransformer{

  override val outcomeName: String = "heart_failure"

}
