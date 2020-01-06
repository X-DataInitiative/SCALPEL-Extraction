// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import fr.polytechnique.cmap.cnam.etl.transformers.TransformerConfig

class ExposuresTransformerConfig(
  val exposurePeriodAdder: ExposurePeriodAdder) extends TransformerConfig with Serializable