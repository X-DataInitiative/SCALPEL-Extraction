// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import fr.polytechnique.cmap.cnam.etl.events.{Event, Interaction}
import cats.syntax.functor._

case class ExposureN(patientID: String, values: Set[String], period: Period) extends
  Ordering[ExposureN] with Remainable[ExposureN] {
  self =>

  def isElevatable(other: ExposureN): Boolean = {
    self.patientID.equals(other.patientID) &&
      self.values.intersect(other.values).isEmpty &&
      (self.period & other.period)
  }

  def intersect(other: ExposureN): ExposureN = {
    ExposureN(patientID, self.values ++ other.values, self.period.merge(other.period))
  }

  def toInteraction: Event[Interaction] =
    Interaction(
      self.patientID,
      ExposureN.nonOrderedValueFormatter(self.values),
      self.values.size.toDouble,
      self.period.start,
      self.period.end
    )

  override def compare(x: ExposureN, y: ExposureN): Int = x.values.size.compareTo(y.values.size)

  def toLowerLevelInvolvedExposureN: Iterator[ExposureN] = {
    self.values.subsets(self.values.size - 1).map(vs => ExposureN(self.patientID, vs, self.period))
  }

  override def - (other: ExposureN): RemainingPeriod[ExposureN] = ExposureN.minus(self, other)
}

object ExposureN {
  def minus(left: ExposureN, right: ExposureN): RemainingPeriod[ExposureN] = {
    if (left.patientID == right.patientID && left.values.equals(right.values)) {
      (left.period - right.period).map(e => ExposureN(left.patientID, left.values, e))
    } else {
      NullRemainingPeriod
    }
  }

  def nonOrderedValueFormatter(values: Set[String]): String =
    values.toList.sorted.reduce((l, r) => l.concat("_").concat(r))
}
