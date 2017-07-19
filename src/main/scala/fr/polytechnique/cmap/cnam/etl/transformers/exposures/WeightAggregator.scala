package fr.polytechnique.cmap.cnam.etl.transformers.exposures

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

  // todo: refactor the parametrization (maybe passing a single config object).
  // The current approach is not maintainable nor scalable
  def aggregateWeight(
      studyStart: Option[Timestamp] = None,
      cumWindow: Option[Int] = None,
      cumStartThreshold: Option[Int] = None,
      cumEndThreshold: Option[Int] = None,
      dosageLevelIntervals: Option[List[Int]] = None,
      purchaseIntervals: Option[List[Int]] = None): DataFrame
}