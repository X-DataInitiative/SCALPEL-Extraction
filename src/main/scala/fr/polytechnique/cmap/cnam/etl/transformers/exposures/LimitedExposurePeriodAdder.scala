package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


private class LimitedExposurePeriodAdder(data: DataFrame) extends ExposurePeriodAdderImpl(data) {

  import Columns._

  private val window = Window.partitionBy(col(PatientID), col(Value))
  private val orderedWindow = window.orderBy(col(Start))

  implicit class InnerImplicits(innerData: DataFrame) {

    def withNextDate: DataFrame = innerData.withColumn("nextDate", lead(col(Start), 1).over(orderedWindow))

    def getTracklosses(endThresholdGc: Period = 3.months, endThresholdNgc: Period = 1.months): DataFrame = {
      innerData
        .withColumn("rank", row_number().over(orderedWindow)) // This is used to find the first line of the window
        .withColumn("startWithThreshold",
        when(col("weight") === 1, col(Start).addPeriod(endThresholdGc)).otherwise(col(Start).addPeriod(endThresholdNgc)))
        .where(
          (col("nextDate").isNull) || // The last line of the ordered window (lead(col("start")) == null)
          (col("rank") === 1) || // The first line of the ordered window
          (col("startWithThreshold") < col("nextDate"))
        )

        .withColumn(Start, when(col("rank") === 1, col(Start).subPeriod(1.day)).otherwise(col(Start)))
        .select(col(PatientID), col(Value), col(Start))
        .withColumn(TracklossDate, lead(col(Start), 1).over(orderedWindow))
        .where(col(TracklossDate).isNotNull)
    }

    def withExposureEnd(tracklosses: DataFrame, endDelay: Period = 0.months): DataFrame = {

      // I needed this redefinition of names because I was getting some very weird errors when using
      //   the .as() function and .select("table.*")
      // Todo: try to understand the problem and solve it
      val adjustedTracklosses = tracklosses.select(
        col(PatientID).as("t_patientID"),
        col(Value).as("t_moleculeName"),
        col(Start).as("t_eventDate"),
        col(TracklossDate)
      )

      // For every row in the tracklosses DataFrame, we will take the trackloss date and add it as
      //   the exposureEnd date for every purchase that happened between the previous trackloss and
      //   the current one.
      val joinConditions =
      col(PatientID) === col("t_patientID") &&
        col(Value) === col("t_moleculeName") &&
        col(Start) > col("t_eventDate") &&
        col(Start) < col(TracklossDate)

      innerData
        .join(adjustedTracklosses, joinConditions, "inner")
        .withColumnRenamed(TracklossDate, ExposureEnd)
        .withColumn(ExposureEnd, col(ExposureEnd).addPeriod(endDelay))
    }

    def withExposureStart(purchasesWindow: Period = 6.months, minPurchases: Int = 2): DataFrame = {
      val window = Window.partitionBy(PatientID, Value, ExposureEnd)

      // We take the first pair of purchases that happened within the threshold and set the
      //   the exposureStart date as the date of the second purchase of the pair.
      val adjustedNextDate: Column =
      if (minPurchases == 1)
        col(Start)
      else
        when(col(Start).addPeriod(purchasesWindow) >= col("nextDate"), col("nextDate"))
      innerData.withColumn(ExposureStart, min(adjustedNextDate).over(window))
    }
  }

  def withStartEnd(
      minPurchases: Int = 2,
      startDelay: Period = 3.months,
      purchasesWindow: Period = 4.months,
      endThresholdGc: Option[Period] = Some(3.months),
      endDelay: Option[Period] = Some(0.months),
      endThresholdNgc: Option[Period] = Some(1.months))
    : DataFrame = {

    val outputColumns = (data.columns.toList ++ List(ExposureStart, ExposureEnd)).map(col)

    val eventsWithDelta = data
      .withNextDate
      .persist()

    val tracklosses = eventsWithDelta.getTracklosses(endThresholdGc.get, endThresholdNgc.get)

    val result = eventsWithDelta
      .withExposureEnd(tracklosses, endDelay.get)
      .withExposureStart(purchasesWindow, minPurchases)

    eventsWithDelta.unpersist()
    result.select(outputColumns: _*)
  }
}
