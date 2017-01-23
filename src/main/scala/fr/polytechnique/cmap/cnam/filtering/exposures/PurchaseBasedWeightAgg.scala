package fr.polytechnique.cmap.cnam.filtering.exposures

import java.sql.Timestamp
import java.util.Calendar
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS

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

