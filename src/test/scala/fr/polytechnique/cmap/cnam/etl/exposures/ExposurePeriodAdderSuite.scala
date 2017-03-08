package fr.polytechnique.cmap.cnam.etl.exposures

import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec

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
