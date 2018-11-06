package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame}
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._

private class UnlimitedExposurePeriodAdder(data: DataFrame) extends ExposurePeriodAdderImpl(data) {

  import Columns._

  def withStartEnd(minPurchases: Int = 2, startDelay: Period = 3.months, purchasesWindow: Period = 6.months, endThresholdGc: Option[Period] = None, endThresholdNgc: Option[Period] = None, endDelay: Option[Period] = None): DataFrame = {

    val window = Window.partitionBy(PatientID, Value)

    val exposureStartRule: Column = when(
      (col("previousStartDate").addPeriod(purchasesWindow) >= col(Start)) ,
      (col(Start).addPeriod(startDelay)).cast(TimestampType)
    )

    val potentialExposureStart: Column = if(minPurchases == 1)
      col(Start)
    else
      lag(col(Start), minPurchases - 1).over(window.orderBy(Start))

    data
      .withColumn("previousStartDate", potentialExposureStart)
      .withColumn(ExposureStart, exposureStartRule)
      .withColumn(ExposureStart, when(col(ExposureStart) < col(FollowUpStart),
        col(FollowUpStart)).otherwise(col(ExposureStart))
      )
      .withColumn(ExposureStart, min(ExposureStart).over(window))
      .withColumn(ExposureEnd, col(FollowUpEnd))
      .drop("previousStartDate")
  }
}
