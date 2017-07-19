package fr.polytechnique.cmap.cnam.etl.transform.exposures

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


class PurchaseBasedWeightAgg(data: DataFrame) extends WeightAggregatorImpl(data) {

  import Columns._

  private def aggregateWeightImpl(purchaseIntervals: List[Int]): DataFrame = {

    val window = Window.partitionBy(PatientID, Value)
    val finalWindow = Window.partitionBy(PatientID, Value, Weight)

    val getLevel = udf {
      (weight: Double) => purchaseIntervals.count(_ <= weight).toDouble
    }

    data
      .withColumn(ExposureStart, col(Start)) // temporary (todo: "first-only" feature in unlimitedPeriodAdder)
      .withColumn(Weight, sum(lit(1.0)).over(window.orderBy(ExposureStart)))
      .withColumn(Weight, getLevel(col(Weight)))
      .withColumn(ExposureStart, min(ExposureStart).over(finalWindow))
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

