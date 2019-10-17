// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.config

import me.danielpes.spark.datetime.Period
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformerConfig

class FracturesTransformerConfig(val fallFrame: Period) extends OutcomesTransformerConfig

object FracturesTransformerConfig {
  def apply(frame: Period): FracturesTransformerConfig = new FracturesTransformerConfig(frame)
}
