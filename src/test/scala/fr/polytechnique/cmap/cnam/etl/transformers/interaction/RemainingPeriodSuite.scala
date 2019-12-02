// License: BSD 3 clause
package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import cats.implicits._
import cats.kernel.Eq
import cats.laws.discipline.FunctorTests
import org.scalatest.FunSuite
import fr.polytechnique.cmap.cnam.Discipline
import org.scalacheck.Shapeless._

class RemainingPeriodSuite extends FunSuite with Discipline {
  implicit def eqRemainingPeriod[A: Eq]: Eq[RemainingPeriod[A]] = Eq.fromUniversalEquals
  checkAll("RemainingPeriod.FunctorLaws", FunctorTests[RemainingPeriod].functor[Int, String, Double])
}

