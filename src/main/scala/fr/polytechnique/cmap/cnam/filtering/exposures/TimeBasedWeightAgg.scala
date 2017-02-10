package fr.polytechnique.cmap.cnam.filtering.exposures

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class TimeBasedWeightAgg(data: DataFrame) extends WeightAggregatorImpl(data) {

  def aggregateWeight(
      studyStart: Option[Timestamp],
      cumWindow: Option[Int],
      cumStartThreshold: Option[Int],
      cumEndThreshold: Option[Int],
      dosageLevelIntervals: Option[List[Int]],
      purchaseIntervals: Option[List[Int]]): DataFrame = {

    val window = Window.partitionBy("patientID", "eventId").orderBy("exposureStart", "exposureEnd")
    data
      .dropDuplicates(Seq("patientID", "eventID", "exposureStart", "exposureEnd"))
      .withColumn("weight", months_between(col("exposureEnd"), col("exposureStart")))
      .withColumn("weight", sum(col("weight")).over(window))
      .withColumn("exposureEnd", col("followUpEnd"))
  }

  def aggregateWeight: DataFrame = aggregateWeight(None, None, None, None, None, None)
}
