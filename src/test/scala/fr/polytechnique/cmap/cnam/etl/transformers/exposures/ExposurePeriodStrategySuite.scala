// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.scalatest.flatspec.AnyFlatSpec

class ExposurePeriodStrategySuite extends AnyFlatSpec {

  "fromString" should "return the correct strategy from a string input" in {
    assert(ExposurePeriodStrategy.fromString("limited") == ExposurePeriodStrategy.Limited)
    assert(ExposurePeriodStrategy.fromString("unlimited") == ExposurePeriodStrategy.Unlimited)
  }
}
