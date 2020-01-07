// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.datatypes

trait Remainable[A] {
  def - (other: A): RemainingPeriod[A]
}
