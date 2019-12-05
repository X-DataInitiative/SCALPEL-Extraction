// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.DataFrame

class ExposurePeriodAdderSuite extends AnyFlatSpec {

  "exposurePeriodImplicits" should "return the correct implementation for the 'Limited' strategy" in {
    val instance = new ExposurePeriodAdder {
      val exposurePeriodStrategy = ExposurePeriodStrategy.Limited
    }.exposurePeriodImplicits(mock(classOf[DataFrame]))
    assert(instance.isInstanceOf[LimitedExposurePeriodAdder])
  }

  it should "return the correct implementation for the 'Unlimited' strategy" in {
    val instance = new ExposurePeriodAdder {
      val exposurePeriodStrategy = ExposurePeriodStrategy.Unlimited
    }.exposurePeriodImplicits(mock(classOf[DataFrame]))
    assert(instance.isInstanceOf[UnlimitedExposurePeriodAdder])
  }
}
