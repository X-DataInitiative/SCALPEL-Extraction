package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

object GeneralFractures extends OutcomeTransformer with FractureCodes {

  override val outcomeName: String = "generic_fall"

}
