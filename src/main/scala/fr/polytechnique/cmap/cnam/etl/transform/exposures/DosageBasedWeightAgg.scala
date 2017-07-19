package fr.polytechnique.cmap.cnam.etl.transform.exposures

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class DosageBasedWeightAgg(data: DataFrame) extends WeightAggregatorImpl(data) {
  import Columns._

  private def aggregateWeightImpl(dosageLevelIntervals: List[Int]): DataFrame = {

    val window = Window.partitionBy(PatientID, Value)
    val finalWindow = Window.partitionBy(PatientID, Value, Weight)

    val getLevel = udf {
      (weight: Double) => dosageLevelIntervals.count(_ <= weight).toDouble
    }

    data
      .withColumn(ExposureStart, col(Start)) // temporary (todo: "first-only" feature in unlimitedPeriodAdder)
      .withColumn(Weight, sum(Weight).over(window.orderBy(ExposureStart)))
      .withColumn(Weight, getLevel(col(Weight)))
      .withColumn(ExposureStart, min(ExposureStart).over(finalWindow))
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
