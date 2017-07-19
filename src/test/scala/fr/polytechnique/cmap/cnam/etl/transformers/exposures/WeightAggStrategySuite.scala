package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.scalatest.FlatSpec

class WeightAggStrategySuite extends FlatSpec {

  "fromString" should "return the correct strategy from a string input" in {
    assert(WeightAggStrategy.fromString("non-cumulative") == WeightAggStrategy.NonCumulative)
    assert(WeightAggStrategy.fromString("NonCumulative") == WeightAggStrategy.NonCumulative)
    assert(WeightAggStrategy.fromString("purchase-based") == WeightAggStrategy.PurchaseBased)
    assert(WeightAggStrategy.fromString("PurchaseBased") == WeightAggStrategy.PurchaseBased)
    assert(WeightAggStrategy.fromString("dosage-based") == WeightAggStrategy.DosageBased)
    assert(WeightAggStrategy.fromString("DosageBased") == WeightAggStrategy.DosageBased)
    assert(WeightAggStrategy.fromString("time-based") == WeightAggStrategy.TimeBased)
    assert(WeightAggStrategy.fromString("TimeBased") == WeightAggStrategy.TimeBased)
  }
}
