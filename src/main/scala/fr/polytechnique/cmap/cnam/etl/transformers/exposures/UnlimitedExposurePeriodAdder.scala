package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame}

private class UnlimitedExposurePeriodAdder(data: DataFrame) extends ExposurePeriodAdderImpl(data) {

  import Columns._

  // TODO: This implementation is buggy for minpurchases of 3 or more
  def withStartEnd(
    minPurchases: Int = 2,
    startDelay: Period = 3.months,
    purchasesWindow: Period = 6.months,
    endThresholdGc: Option[Period] = None,
    endDelay: Option[Period] = None,
    endThresholdNgc: Option[Period] = None): DataFrame = {

    val window = Window.partitionBy(PatientID, Value)

    val exposureStartRule: Column = when(
      col("previousStartDate").addPeriod(purchasesWindow) >= col(Start),
      col(Start).addPeriod(startDelay).cast(TimestampType)
    )

    val potentialExposureStart: Column = if (minPurchases == 1) {
      col(Start)
    } else {
      lag(col(Start), 1).over(window.orderBy(Start))
    }

    data
      .withColumn("previousStartDate", potentialExposureStart)
      .withColumn(ExposureStart, exposureStartRule)
      .withColumn(
        ExposureStart, when(
          col(ExposureStart) < col(FollowUpStart),
          col(FollowUpStart)
        ).otherwise(col(ExposureStart))
      )
      .withColumn(ExposureStart, min(ExposureStart).over(window))
      .withColumn(ExposureEnd, col(FollowUpEnd))
      .where(col(ExposureStart) <= col(ExposureEnd)) // Due to some conflicting parameters with Followup Config, this
      //might happen. This line is strictly defensive, and should be removed.
      .drop("previousStartDate")
  }
}
