package fr.polytechnique.cmap.cnam.filtering.exposures

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame

trait WeightAggregator {

  val weightAggStrategy: WeightAggStrategy

  implicit def weightCalculationImplicits(data: DataFrame): WeightAggregatorImpl = {
    weightAggStrategy match {
      case WeightAggStrategy.NonCumulative => new NonCumulativeWeightAgg(data)
      case WeightAggStrategy.PurchaseBased => new PurchaseBasedWeightAgg(data)
      case WeightAggStrategy.DosageBased => new DosageBasedWeightAgg(data)
      case WeightAggStrategy.TimeBased => new TimeBasedWeightAgg(data)
    }
  }
}

abstract class WeightAggregatorImpl(data: DataFrame) {

  def aggregateWeight(
      studyStart: Option[Timestamp],
      cumWindow: Option[Int],
      cumStartThreshold: Option[Int],
      cumEndThreshold: Option[Int]): DataFrame
}