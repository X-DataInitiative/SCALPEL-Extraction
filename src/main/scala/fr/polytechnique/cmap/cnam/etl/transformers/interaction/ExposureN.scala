// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import cats.syntax.functor._
import me.danielpes.spark.datetime.{Period => Duration}
import fr.polytechnique.cmap.cnam.etl.datatypes.{NullRemainingPeriod, Period, Subtractable, RemainingPeriod}
import fr.polytechnique.cmap.cnam.etl.events.{Event, Interaction}

case class ExposureN(patientID: String, values: Set[String], period: Period) extends Subtractable[ExposureN] {
  self =>

  /**
   * Returns duration of this ExposureN in milliseconds
   * @return duration in millisecond as Long
   */
  def toDuration: Long = self.period.end.getTime - self.period.start.getTime

  def intersect(other: ExposureN): Option[ExposureN] = {
    if (self.patientID.equals(other.patientID) &&
      self.values.intersect(other.values).isEmpty) {
      self.period.intersect(other.period).map(p => ExposureN(patientID, self.values ++ other.values, p))
    } else {
      None
    }
  }

  def toInteraction: Event[Interaction] =
    Interaction(
      self.patientID,
      ExposureN.nonOrderedValueFormatter(self.values),
      self.values.size.toDouble,
      self.period.start,
      self.period.end
    )

  def toLowerLevelInvolvedExposureN: Iterator[ExposureN] = {
    self.values.subsets(self.values.size - 1).map(vs => ExposureN(self.patientID, vs, self.period))
  }

  override def -(other: ExposureN): RemainingPeriod[ExposureN] = ExposureN.minus(self, other)
}

object ExposureN extends Ordering[ExposureN] {
  override def compare(x: ExposureN, y: ExposureN): Int = x.values.size.compareTo(y.values.size)

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
