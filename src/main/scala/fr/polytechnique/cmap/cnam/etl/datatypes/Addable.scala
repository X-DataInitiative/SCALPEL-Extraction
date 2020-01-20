// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.datatypes

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait Addable [A] {
  def + (other: A): RemainingPeriod[A]
}

object Addable {
  /***
    * Transforms a list of `Addable` of type A into a list of A where every element do not intersect with any other
    * element of the list. The passed list of elements must ordered from the first to the last.
    * To achieve the non intersection property, the algorithm proceedes as the following:
    * 1. Takes an element of `RightRemainingPeriod` and add it to head of List of `LeftRemainingPeriod`.
    *   1. If the result is `RightRemainingPeriod`, recurse with the new element as the `RightRemainingPeriod`.
    *   2. If the result is `LeftRemainingPeriod`, add it to the Accumulator, and recurse with the head of of List of
    *   `LeftRemainingPeriod` as the new `RightRemainingPeriod`.
    *   3. If the result is `DisjointedRemainingPeriod`, add the Left part to the Accumulator,
    *   and recurse with the right part.
    *   4. If the result is `NullRemainingPeriod`, add the element of `RightRemainingPeriod` to the Accumulator,
    *   and recurse with the head of of List of `LeftRemainingPeriod` as the new `RightRemainingPeriod`
    * 2. Recurse until lrs is empty.
    * @param rr element to start the add operation with
    * @param lrs list of LeftRemainingPeriod to combine. Must be timely ordered with respect to definition of `+` of type A.
    * @param acc list to accumulate the results in.
    * @tparam A type parameter. A must extend the Addable trait.
    * @return List of `LeftRemainingPeriod` of type A.
    */
  @tailrec
  def combineAddables[A <: Addable[A] : ClassTag : TypeTag](
    rr: RightRemainingPeriod[A],
    lrs: List[LeftRemainingPeriod[A]],
    acc: List[LeftRemainingPeriod[A]]): List[LeftRemainingPeriod[A]] = {
    lrs match {
      case Nil => rr.toLeft :: acc
      case lr :: Nil => rr.e + lr.e match {
        case NullRemainingPeriod => acc
        case l: LeftRemainingPeriod[A] => l :: acc
        case r: RightRemainingPeriod[A] => combineAddables[A](r, List.empty, acc)
        case d: DisjointedRemainingPeriod[A] => combineAddables[A](d.r, List.empty, d.l :: acc)
      }
      case lr :: lr2 :: rest => rr.e + lr.e match {
        case NullRemainingPeriod => combineAddables(lr2.toRight, rest, acc)
        case l: LeftRemainingPeriod[A] => combineAddables(lr2.toRight, rest, l :: acc)
        case r: RightRemainingPeriod[A] => combineAddables[A](r, lr2 :: rest, acc)
        case d: DisjointedRemainingPeriod[A] => combineAddables[A](d.r, lr2 :: rest, d.l :: acc)
      }
    }
  }
}