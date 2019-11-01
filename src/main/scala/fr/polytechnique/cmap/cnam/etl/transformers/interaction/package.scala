// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers

import java.sql.Timestamp
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import cats.syntax.functor._
import cats.Functor
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure, Interaction}

package object interaction {
  case class InterActionPeriod(patientID: String, value: String, start: Timestamp, end: Timestamp)

  implicit val ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  type Interaction_ = Event[Interaction]
  type Exposure_ = Event[Exposure]
  type ElevatedExposure = (Exposure_, Exposure_)

  type Interactions = Dataset[Interaction_]
  type Exposures = Dataset[Exposure_]
  type ElevatedExposures = Dataset[ElevatedExposure]
  type InterActionPeriods = Dataset[InterActionPeriod]

  val min = (x: Timestamp, y: Timestamp) => if (ordered.lteq(x, y)) x else y
  val max = (x: Timestamp, y: Timestamp) => if (ordered.gteq(x, y)) x else y

  object RemainingPeriod {
    implicit val remainingPeriodFunctor = new Functor[RemainingPeriod] {
      override def map[A, B](fa: RemainingPeriod[A])(f: A => B): RemainingPeriod[B] = {
        fa match {
          case RightRemainingPeriod(e) => RightRemainingPeriod(f(e))
          case LeftRemainingPeriod(e) => LeftRemainingPeriod(f(e))
          case DisjointedRemainingPeriod(LeftRemainingPeriod(e), RightRemainingPeriod(d))
          => DisjointedRemainingPeriod(LeftRemainingPeriod(f(e)), RightRemainingPeriod(f(d)))
          case NullRemainingPeriod => NullRemainingPeriod
        }
      }
    }

    @tailrec
    def delimitPeriods[A <: Remainable[A] : ClassTag : TypeTag](
      rr: RightRemainingPeriod[A],
      lrs: List[LeftRemainingPeriod[A]],
      acc: List[LeftRemainingPeriod[A]]): List[LeftRemainingPeriod[A]] = {
      lrs match {
        case Nil => rr.toLeft :: acc
        case _@LeftRemainingPeriod(null) :: Nil => rr.toLeft :: acc // This happens because of the Left Join
        case i :: rest => rr.e - i.e match {
          case NullRemainingPeriod => acc
          case l: LeftRemainingPeriod[A] => l :: acc
          case r: RightRemainingPeriod[A] => delimitPeriods[A](r, rest, acc)
          case d: DisjointedRemainingPeriod[A] => delimitPeriods[A](d.r, rest, d.l :: acc)
        }
      }
    }
  }

  sealed trait RemainingPeriod[+A]

  case class RightRemainingPeriod[A](e: A) extends RemainingPeriod[A] {
    def toLeft: LeftRemainingPeriod[A] = LeftRemainingPeriod(e)
  }

  case class LeftRemainingPeriod[A](e: A) extends RemainingPeriod[A] {
    def toRight: RightRemainingPeriod[A] = RightRemainingPeriod[A](e)
  }

  case class DisjointedRemainingPeriod[A](
    l: LeftRemainingPeriod[A],
    r: RightRemainingPeriod[A]) extends RemainingPeriod[A]

  case object NullRemainingPeriod extends RemainingPeriod[Nothing]

  trait Remainable[A] {
    def - (other: A): RemainingPeriod[A]
  }

  case class Period(start: Timestamp, end: Timestamp) extends Remainable[Period]{
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

    def merge(other: Period): Period = Period(max(self.start, other.start), min(self.end, other.end))

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
  }

  object Period extends Ordering[Period] {
    override def compare(
      x: Period,
      y: Period): Int = x.start.compareTo(y.start)
  }

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

    override def compare(
      x: ExposureN,
      y: ExposureN): Int = x.values.size.compareTo(y.values.size)

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
}
