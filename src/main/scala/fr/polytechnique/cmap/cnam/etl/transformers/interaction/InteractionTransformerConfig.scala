// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import fr.polytechnique.cmap.cnam.etl.transformers.TransformerConfig

class InteractionTransformerConfig(val level: Int) extends TransformerConfig

object InteractionTransformerConfig {
  def apply(level: Int = 3): InteractionTransformerConfig = new InteractionTransformerConfig(level)
}
