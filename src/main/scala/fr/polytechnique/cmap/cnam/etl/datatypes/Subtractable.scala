// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.datatypes

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait Subtractable[A] {
  def - (other: A): RemainingPeriod[A]
}

object Subtractable {
  /***
    * Subtract a list of `LeftRemainingPeriod` A from RightRemainingPeriod of type A. It proceedes until it is not
    * possible to subtract.
    * To achieve the non subtraction property, the algorithm proceeds as the following:
    * 1. Takes the element of `RightRemainingPeriod` and subtract the head of List of `LeftRemainingPeriod` from it.
    *   1. If the result is `RightRemainingPeriod`, recurse with the remaining element as the `RightRemainingPeriod`.
    *   2. If the result is `LeftRemainingPeriod`, add it to the Accumulator, stop.
    *   3. If the result is `DisjointedRemainingPeriod`, add the Left part to the Accumulator,
    *   and recurse with the right part.
    *   4. If the result is `NullRemainingPeriod`, return the acc and stop it.
    * 2. Recurse until lrs is empty.
    * @param rr element to start the add operation with
    * @param lrs list of LeftRemainingPeriod to combine. Must be timely ordered with respect to definition of `-` of type A.
    * @param acc list to accumulate the results in.
    * @tparam A type parameter. A must extend the Subtractable trait.
    * @return List of `LeftRemainingPeriod` of type A.
    */
  @tailrec
  def combineSubtracables[A <: Subtractable[A] : ClassTag : TypeTag](
    rr: RightRemainingPeriod[A],
    lrs: List[LeftRemainingPeriod[A]],
    acc: List[LeftRemainingPeriod[A]]): List[LeftRemainingPeriod[A]] = {
    lrs match {
      case Nil => rr.toLeft :: acc
      case _@LeftRemainingPeriod(null) :: Nil => rr.toLeft :: acc // This happens because of the Left Join
      case lr :: rest => rr.e - lr.e match {
        case NullRemainingPeriod => acc
        case l: LeftRemainingPeriod[A] => l :: acc
        case r: RightRemainingPeriod[A] => combineSubtracables[A](r, rest, acc)
        case d: DisjointedRemainingPeriod[A] => combineSubtracables[A](d.r, rest, d.l :: acc)
      }
    }
  }
}