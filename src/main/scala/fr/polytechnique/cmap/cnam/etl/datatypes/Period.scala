// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.datatypes

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.util.functions._

case class Period(start: Timestamp, end: Timestamp) extends Subtractable[Period] with Addable[Period]{
  self =>

  def & (other: Period): Boolean = {
    (self.start.compareTo(other.start), self.end.compareTo(other.end)) match {
      case (0, _) => true
      case (_, 0) => true
      case (1, -1) => true
      case (-1, 1) => true
      case (1, 1) => self.start.compareTo(other.end) match {
        case 0 => false
        case 1 => false
        case -1 => true
      }
      case (-1, -1) => self.end.compareTo(other.start) match {
        case 0 => false
        case -1 => false
        case 1 => true
      }
    }
  }

  def intersect(other: Period): Option[Period] = {
    if (self & other) {
      Some(Period(max(self.start, other.start), min(self.end, other.end)))
    } else {
      None
    }
  }

  def - (other: Period): RemainingPeriod[Period] = {
    (self.start.compareTo(other.start), self.end.compareTo(other.end)) match {
      case (0, 0) => NullRemainingPeriod
      case (0, 1) => RightRemainingPeriod(Period(other.end, self.end))
      case (0, -1) => NullRemainingPeriod
      case (1, 0) => NullRemainingPeriod
      case (1, 1) => self.start.compareTo(other.end) match {
        case 0 => RightRemainingPeriod(self)
        case 1 => RightRemainingPeriod(self)
        case -1 => RightRemainingPeriod(Period(other.end, self.end))
      }
      case (1, -1) => NullRemainingPeriod
      case (-1, 0) => LeftRemainingPeriod(Period(self.start, other.start))
      case (-1, 1) => DisjointedRemainingPeriod(
        LeftRemainingPeriod(Period(self.start, other.start)),
        RightRemainingPeriod(Period(other.end, self.end))
      )
      case (-1, -1) => self.end.compareTo(other.start) match {
        case 0 => LeftRemainingPeriod(self)
        case -1 => LeftRemainingPeriod(self)
        case 1 => LeftRemainingPeriod(Period(self.start, other.start))
      }
    }
  }

  def + (other: Period): RemainingPeriod[Period] = {
    if (self & other) {
      RightRemainingPeriod(Period(min(self.start, other.start), max(self.end, other.end)))
    } else {
      (self.start.compareTo(other.start), self.end.compareTo(other.end)) match {
        case (1, 1) => DisjointedRemainingPeriod(LeftRemainingPeriod(other), RightRemainingPeriod(self))
        case (-1, -1) => DisjointedRemainingPeriod(LeftRemainingPeriod(self), RightRemainingPeriod(other))
      }
    }
  }
}

object Period extends Ordering[Period] {
  override def compare(x: Period, y: Period): Int = x.start.compareTo(y.start)
}
