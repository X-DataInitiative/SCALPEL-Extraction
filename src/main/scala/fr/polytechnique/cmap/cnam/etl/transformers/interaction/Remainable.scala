// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

trait Remainable[A] {
  def - (other: A): RemainingPeriod[A]
}
