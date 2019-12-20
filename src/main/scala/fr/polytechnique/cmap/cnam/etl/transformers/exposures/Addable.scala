// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import fr.polytechnique.cmap.cnam.etl.transformers.interaction.RemainingPeriod

trait Addable [A] {
  def + (other: A): RemainingPeriod[A]
}
