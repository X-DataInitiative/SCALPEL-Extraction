package fr.polytechnique.cmap.cnam.filtering.exposures

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

private class UnlimitedExposurePeriodAdder(data: DataFrame) extends ExposurePeriodAdderImpl(data) {

  def withStartEnd(minPurchases: Int = 2, startDelay: Int = 3, purchasesWindow: Int = 6): DataFrame = {

    val window = Window.partitionBy("patientID", "eventId")

    val exposureStartRule: Column = when(
      months_between(col("start"), col("previousStartDate")) <= purchasesWindow,
      add_months(col("start"), startDelay).cast(TimestampType)
    )

    val potentialExposureStart: Column = if(minPurchases == 1)
      col("start")
    else
      lag(col("start"), minPurchases - 1).over(window.orderBy("start"))

    data
      .withColumn("previousStartDate", potentialExposureStart)
      .withColumn("exposureStart", exposureStartRule)
      .withColumn("exposureStart", when(col("exposureStart") < col("followUpStart"),
        col("followUpStart")).otherwise(col("exposureStart"))
      )
      .withColumn("exposureStart", min("exposureStart").over(window))
      .withColumn("exposureEnd", col("followUpEnd"))
      .drop("previousStartDate")
  }
}
