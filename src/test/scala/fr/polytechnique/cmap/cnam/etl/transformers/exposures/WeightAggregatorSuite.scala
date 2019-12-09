// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.DataFrame

class WeightAggregatorSuite extends AnyFlatSpec {

  "weightCalculationImplicits" should "return the correct implementation for the 'NonCumulative' strategy" in {
    val instance = new WeightAggregator {
      val weightAggStrategy = WeightAggStrategy.NonCumulative
    }.weightCalculationImplicits(mock(classOf[DataFrame]))
    assert(instance.isInstanceOf[NonCumulativeWeightAgg])
  }

  it should "return the correct implementation for the 'PurchaseBased' strategy" in {
    val instance = new WeightAggregator {
      val weightAggStrategy = WeightAggStrategy.PurchaseBased
    }.weightCalculationImplicits(mock(classOf[DataFrame]))
    assert(instance.isInstanceOf[PurchaseBasedWeightAgg])
  }

  it should "return the correct implementation for the 'DosageBased' strategy" in {
    val instance = new WeightAggregator {
      val weightAggStrategy = WeightAggStrategy.DosageBased
    }.weightCalculationImplicits(mock(classOf[DataFrame]))
    assert(instance.isInstanceOf[DosageBasedWeightAgg])
  }

  it should "return the correct implementation for the 'TimeBased' strategy" in {
    val instance = new WeightAggregator {
      val weightAggStrategy = WeightAggStrategy.TimeBased
    }.weightCalculationImplicits(mock(classOf[DataFrame]))
    assert(instance.isInstanceOf[TimeBasedWeightAgg])
  }
}
