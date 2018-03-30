package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


class TimeBasedWeightAgg(data: DataFrame) extends WeightAggregatorImpl(data) {

  import Columns._

  def aggregateWeight(
      cumWindow: Option[Int],
      cumStartThreshold: Option[Int],
      cumEndThreshold: Option[Int],
      dosageLevelIntervals: Option[List[Int]],
      purchaseIntervals: Option[List[Int]]): DataFrame = {

    val window = Window.partitionBy(PatientID, Value).orderBy(ExposureStart, ExposureEnd)
    data
      .dropDuplicates(Seq(PatientID, Value, ExposureStart, ExposureEnd))
      .withColumn(Weight, months_between(col(ExposureEnd), col(ExposureStart)))
      .withColumn(Weight, sum(col(Weight)).over(window))
      .withColumn(ExposureEnd, col(FollowUpEnd))
  }

  def aggregateWeight: DataFrame = aggregateWeight(None, None, None, None, None)
}
