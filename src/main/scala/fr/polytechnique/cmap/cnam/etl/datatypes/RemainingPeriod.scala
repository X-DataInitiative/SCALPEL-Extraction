// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.datatypes

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
}

