package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

object GenericFall extends OutcomeTransformer with FallStudyCodes {

  override val outcomeName: String = "generic_fall"

}
