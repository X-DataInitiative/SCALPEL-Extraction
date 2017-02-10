package fr.polytechnique.cmap.cnam.filtering.exposures

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class DosageBasedWeightAgg(data: DataFrame) extends WeightAggregatorImpl(data) {

  private def aggregateWeightImpl(dosageLevelIntervals: List[Int]): DataFrame = {

    val window = Window.partitionBy("patientID", "eventId")
    val finalWindow = Window.partitionBy("patientID", "eventId", "weight")

    val getLevel = udf {
      (weight: Double) => dosageLevelIntervals.count(_ <= weight).toDouble
    }

    data
      .withColumn("exposureStart", col("start")) // temporary (todo: "first-only" feature in unlimitedPeriodAdder)
      .withColumn("weight", (sum("weight").over(window.orderBy("exposureStart"))))
      .withColumn("weight", getLevel(col("weight")))
      .withColumn("exposureStart", min("exposureStart").over(finalWindow))
  }
  def aggregateWeight(
      studyStart: Option[Timestamp] = None,
      cumWindow: Option[Int] = None,
      cumStartThreshold: Option[Int] = None,
      cumEndThreshold: Option[Int] = None,
      dosageLevelIntervals: Option[List[Int]],
      purchaseIntervals: Option[List[Int]]): DataFrame = {

    aggregateWeightImpl(dosageLevelIntervals.get)
  }
}
