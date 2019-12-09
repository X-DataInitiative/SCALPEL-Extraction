// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import cats.Functor


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


object RemainingPeriod {
  implicit val remainingPeriodFunctor: Functor[RemainingPeriod] = new Functor[RemainingPeriod] {
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
      case lr :: rest => rr.e - lr.e match {
        case NullRemainingPeriod => acc
        case l: LeftRemainingPeriod[A] => l :: acc
        case r: RightRemainingPeriod[A] => delimitPeriods[A](r, rest, acc)
        case d: DisjointedRemainingPeriod[A] => delimitPeriods[A](d.r, rest, d.l :: acc)
      }
    }
  }
}

