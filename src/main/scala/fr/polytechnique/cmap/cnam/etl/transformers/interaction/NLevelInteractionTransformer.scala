// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import java.sql.Timestamp
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try
import cats.Functor
import cats.syntax.functor._
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure, Interaction}

case class NLevelInteractionTransformer(level: Int) extends InteractionTransformer {
  self =>

  def joinTwoExposureN(right: Dataset[ExposureN], left: Dataset[ExposureN]): Dataset[ExposureN]= {
    val sqlCtx = right.sqlContext
    import sqlCtx.implicits._
    right
      .joinWith(left, left(Event.Columns.PatientID) === right(Event.Columns.PatientID))
      .filter(e => e._1.isElevatable(e._2)).map(e => e._1.merge(e._2)).distinct()
  }


  def elevateExposureN(exposures: Exposures, n: Int): List[Dataset[ExposureN]] = {
    val sqlCtx = exposures.sqlContext
    import sqlCtx.implicits._

    val zero = exposures.map(e => ExposureN(e.patientID, Set(e.value), Period(e.start, e.end.get)))

    List
      .fill(n - 1)(exposures.map(e => ExposureN(e.patientID, Set(e.value), Period(e.start, e.end.get))))
      .scanRight(zero)(joinTwoExposureN)
  }

  def trickleDownExposureN(ens: List[Dataset[ExposureN]]): List[Dataset[ExposureN]] = {
    val sqlCtx = ens.head.sqlContext
    import sqlCtx.implicits._

    ens.filter(
      dataset =>
        dataset.take(1).apply(0).values.size > 1
    )
      .map(dataset => dataset.flatMap(e => e.toLowerLevelInvolvedExposureN))
  }

  def reduceHigherExposuresNFromLowerExposures(higher: List[Dataset[ExposureN]], lower: List[Dataset[ExposureN]])
  : List[Dataset[LeftRemainingPeriod[ExposureN]]] = {
    val sqlCtx = higher.head.sqlContext
    import sqlCtx.implicits._
    lower.zip(higher).map(
      e => {
        val down1 = e._1
        val high = e._2
        high.joinWith(
          down1,
          high("patientID") === down1("patientID") && high("values") === down1("values"),
          "left"
        )
      }
    ).map(
      dataset => dataset
        .groupByKey(e => e._1)
        .flatMapGroups(
          (e, i) =>
            RemainingPeriod.delimitPeriods(
              RightRemainingPeriod(e),
              i.map(_._2).toList.sortBy(_.period.start).map(e => LeftRemainingPeriod[ExposureN](e)),
              List.empty[LeftRemainingPeriod[ExposureN]]
            )
        )
    )
  }

  override def transform(exposures: Dataset[Event[Exposure]]): Dataset[Event[Interaction]] = {
    val exposureN = self.elevateExposureN(exposures, level)

    val sqlCtx = exposures.sqlContext
    import sqlCtx.implicits._
    self
      .reduceHigherExposuresNFromLowerExposures(exposureN.drop(1), self.trickleDownExposureN(exposureN))
      .foldLeft(exposureN.head.map(l => l.toInteraction))((acc, b) => acc.union(b.map(l => l.e.toInteraction)))
      .distinct()
  }
}

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
      case i :: rest => rr.e.-(i.e) match {
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


case class Period(start: Timestamp, end: Timestamp) {
  self =>


  def intersect(other: Period): Boolean = List(self, other).sortBy(_.start) match {
    case left :: right :: Nil => right.start.compareTo(left.end) < 0
  }

  def merge(other: Period): Period = Period(max(self.start, other.start), min(self.end, other.end))

  def -(other: Period): RemainingPeriod[Period] = {
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
      self.period.intersect(other.period)
  }

  def merge(other: ExposureN): ExposureN = {
    ExposureN(patientID, self.values ++ other.values, self.period.merge(other.period))
  }

  def toInteraction: Event[Interaction] =
    Interaction(
      self.patientID,
      self.values.toList.sorted.reduce((l, r) => l.concat("_").concat(r)),
      self.values.size.toDouble,
      self.period.start,
      self.period.end
    )

  override def compare(
    x: ExposureN,
    y: ExposureN): Int = x.values.size.compareTo(y.values.size)

  def fusion(other: ExposureN): Option[ExposureN] = ExposureN.fusion(self, other)

  def toLowerLevelInvolvedExposureN: Iterator[ExposureN] = {
    self.values.subsets(self.values.size - 1).map(vs => ExposureN(self.patientID, vs, self.period))
  }

  override def -(other: ExposureN): RemainingPeriod[ExposureN] = ExposureN.-(self, other)
}

object ExposureN {
  def fusion(left: ExposureN, right: ExposureN): Option[ExposureN] =
    if (areFusionable(left, right)) {
      Some(ExposureN(left.patientID, left.values, left.period.merge(right.period)))
    } else {
      None
    }

  def areFusionable(left: ExposureN, right: ExposureN): Boolean =
    left.patientID == right.patientID &&
      left.values.equals(right.values) &&
      left.period.intersect(right.period)

  def -(self: ExposureN, other: ExposureN): RemainingPeriod[ExposureN] = {
    if (self.patientID == other.patientID && self.values.equals(other.values)) {
      self.period.-(other.period).map(e => ExposureN(self.patientID, self.values, e))
    } else {
      NullRemainingPeriod
    }
  }
}

trait Remainable[A] {
  def -(other: A): RemainingPeriod[A]
}
