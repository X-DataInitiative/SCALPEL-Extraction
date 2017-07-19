package fr.polytechnique.cmap.cnam.etl.old_root.exposures

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class PurchaseBasedWeightAgg(data: DataFrame) extends WeightAggregatorImpl(data) {

  private def aggregateWeightImpl(purchaseIntervals: List[Int]): DataFrame = {

    val window = Window.partitionBy("patientID", "eventId")
    val finalWindow = Window.partitionBy("patientID", "eventId", "weight")

    val getLevel = udf {
      (weight: Double) => purchaseIntervals.count(_ <= weight).toDouble
    }

    data
      .withColumn("exposureStart", col("start")) // temporary (todo: "first-only" feature in unlimitedPeriodAdder)
      .withColumn("weight", (sum(lit(1.0)).over(window.orderBy("exposureStart"))))
      .withColumn("weight", getLevel(col("weight")))
      .withColumn("exposureStart", min("exposureStart").over(finalWindow))
  }

  def aggregateWeight(
    studyStart: Option[Timestamp],
    cumWindow: Option[Int],
    cumStartThreshold: Option[Int] = None,
    cumEndThreshold: Option[Int] = None,
    dosageLevelIntervals: Option[List[Int]]= None,
    purchaseIntervals: Option[List[Int]]): DataFrame = {

    this.aggregateWeightImpl(purchaseIntervals.get)
  }
}

