package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class NonCumulativeWeightAgg(data: DataFrame) extends WeightAggregatorImpl(data) {

  import Columns._

  def aggregateWeight: DataFrame = aggregateWeight(None, None, None, None, None)

  def aggregateWeight(
    cumWindow: Option[Int],
    cumStartThreshold: Option[Int],
    cumEndThreshold: Option[Int],
    dosageLevelIntervals: Option[List[Int]],
    purchaseIntervals: Option[List[Int]]): DataFrame = this.aggregateWeightImpl

  private def aggregateWeightImpl = data.withColumn(Weight, lit(1D))
}
