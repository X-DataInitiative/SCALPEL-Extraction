package fr.polytechnique.cmap.cnam.etl.exposures

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{min, _}
import org.apache.spark.sql.{Column, DataFrame}

private class LimitedExposurePeriodAdder(data: DataFrame) extends ExposurePeriodAdderImpl(data) {

  private val window = Window.partitionBy(col("patientID"), col("eventId"))
  private val orderedWindow = window.orderBy(col("start"))

  implicit class InnerImplicits(innerData: DataFrame) {

    def withNextDate: DataFrame = innerData.withColumn("nextDate", lead(col("start"), 1).over(orderedWindow))

    def withDelta: DataFrame = innerData.withColumn("delta", months_between(col("nextDate"), col("start")))

    def getTracklosses(endThreshold: Int = 4): DataFrame = {
      innerData
        .withColumn("rank", row_number().over(orderedWindow)) // This is used to find the first line of the window
        .where(
          col("nextDate").isNull ||   // The last line of the ordered window (lead(col("start")) == null)
            (col("rank") === 1) ||      // The first line of the ordered window
            (col("delta") > endThreshold)  // All the lines that represent a trackloss
        )
        .select(col("patientID"), col("eventId"), col("start"))
        .withColumn("tracklossDate", lead(col("start"), 1).over(orderedWindow))
        .where(col("tracklossDate").isNotNull)
    }

    def withExposureEnd(tracklosses: DataFrame): DataFrame = {

      // I needed this redefinition of names because I was getting some very weird errors when using
      //   the .as() function and .select("table.*")
      // Todo: try to understand the problem and solve it
      val adjustedTracklosses = tracklosses.select(
        col("patientID").as("t_patientID"),
        col("eventId").as("t_moleculeName"),
        col("start").as("t_eventDate"),
        col("tracklossDate")
      )

      // For every row in the tracklosses DataFrame, we will take the trackloss date and add it as
      //   the exposureEnd date for every purchase that happened between the previous trackloss and
      //   the current one.
      val joinConditions =
      (col("patientID") === col("t_patientID")) &&
        (col("eventId") === col("t_moleculeName")) &&
        (col("start") >= col("t_eventDate")) &&
        (col("start") < col("tracklossDate"))

      innerData
        .join(adjustedTracklosses, joinConditions, "left_outer")
        .withColumnRenamed("tracklossDate", "exposureEnd")
    }

    def withExposureStart(purchasesWindow: Int = 6): DataFrame = {
      val window = Window.partitionBy("patientID", "eventId", "exposureEnd")

      // We take the first pair of purchases that happened within the threshold and set the
      //   the exposureStart date as the date of the second purchase of the pair.
      val adjustedNextDate: Column = when(col("delta") <= purchasesWindow, col("nextDate"))
      innerData.withColumn("exposureStart", min(adjustedNextDate).over(window))
    }
  }

  def withStartEnd(minPurchases: Int = 2, startDelay: Int = 3, purchasesWindow: Int = 6): DataFrame = {

    val outputColumns = (data.columns.toList ++ List("exposureStart", "exposureEnd")).map(col)

    val eventsWithDelta = data
      .withNextDate
      .withDelta
      .persist()

    // todo: get endThreshold as parameter
    val tracklosses = eventsWithDelta.getTracklosses()

    val result = eventsWithDelta
      .withExposureEnd(tracklosses)
      .withExposureStart(purchasesWindow)

    eventsWithDelta.unpersist()
    result.select(outputColumns: _*)
  }
}
