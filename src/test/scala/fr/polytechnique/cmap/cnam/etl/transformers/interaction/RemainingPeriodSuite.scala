// License: BSD 3 clause
package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import cats.implicits._
import cats.kernel.Eq
import cats.laws.discipline.FunctorTests
import org.scalacheck.ScalacheckShapeless._
import org.scalatest.funsuite.AnyFunSuite
import fr.polytechnique.cmap.cnam.Discipline
import fr.polytechnique.cmap.cnam.etl.datatypes.RemainingPeriod

class RemainingPeriodSuite extends AnyFunSuite with Discipline {
  implicit def eqRemainingPeriod[A: Eq]: Eq[RemainingPeriod[A]] = Eq.fromUniversalEquals
  checkAll("RemainingPeriod.FunctorLaws", FunctorTests[RemainingPeriod].functor[Int, String, Double])
}

