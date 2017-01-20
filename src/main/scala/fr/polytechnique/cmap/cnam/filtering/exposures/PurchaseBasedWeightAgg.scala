package fr.polytechnique.cmap.cnam.filtering.exposures

import java.sql.Timestamp
import java.util.Calendar
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS

class PurchaseBasedWeightAgg(data: DataFrame) extends WeightAggregatorImpl(data) {

  private def aggregateWeightImpl(studyStart: Timestamp, cumWindow: Int): DataFrame = {

    val outputColumns: Seq[Column] = data.columns.map(col) :+ col("weight")
    val window = Window.partitionBy("patientID", "eventId")
    val windowCumulativeExposure = window.partitionBy("patientID", "eventId", "exposureStart")

    def normalizeStartMonth(start: Column) = {
      floor(months_between(start, lit(studyStart)) / cumWindow).cast(IntegerType)
    }

    val normalizeStart = udf(
      (normalizedMonth: Int) => {
        val cal: Calendar = Calendar.getInstance()
        cal.setTime(studyStart)
        cal.add(Calendar.MONTH, normalizedMonth * cumWindow)
        makeTS(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, 1)
      }
    )

    data
      .withColumn("normalizedMonth", normalizeStartMonth(col("start")))
      .withColumn("exposureStart", normalizeStart(col("normalizedMonth")))
      .withColumn("weight", row_number().over(window.orderBy("start")))
      .withColumn("weight", max("weight").over(windowCumulativeExposure).cast(DoubleType))
      .select(outputColumns: _*)
  }

  def aggregateWeight(
      studyStart: Option[Timestamp],
      cumWindow: Option[Int],
      cumStartThreshold: Option[Int] = None,
      cumEndThreshold: Option[Int] = None,
      dosageLevelIntervals: Option[List[Int]]= None): DataFrame = {

    this.aggregateWeightImpl(studyStart.get, cumWindow.get)
  }
}
