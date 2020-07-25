// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import me.danielpes.spark.datetime.{Period => Duration}
import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.transformers.TransformerConfig

class InteractionTransformerConfig(val level: Int, val minimumDuration: Duration) extends TransformerConfig

object InteractionTransformerConfig {
  def apply(
    level: Int = 3,
    minimumDuration: Duration = 30.days): InteractionTransformerConfig = new InteractionTransformerConfig(
    level,
    minimumDuration
  )
}
