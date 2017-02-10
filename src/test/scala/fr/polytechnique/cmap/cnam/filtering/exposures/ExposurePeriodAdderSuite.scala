package fr.polytechnique.cmap.cnam.filtering.exposures

import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
import org.mockito.Mockito.mock

class ExposurePeriodAdderSuite extends FlatSpec {

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
