package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


private class LimitedExposurePeriodAdder(data: DataFrame) extends ExposurePeriodAdderImpl(data) {

  import Columns._

  private val window = Window.partitionBy(col(PatientID), col(Value))
  private val orderedWindow = window.orderBy(col(Start))

  /** *
   * This strategy works as the following:
   * 1. Each DrugPurchase will have a corresponding Exposure.
   * 2. Each Exposure has one or multiple DrugPurchases.
   * 3. An Exposure is defined recursively as follows:
   *   A. The first DrugPurchase defines a new Exposure.
   *   B. If there is a DrugPurchase within the defined window of the first DrugPurchase, then expand the current
   * Exposure with the DrugPurchase.
   *   C. Else, close and set the end of the Exposure as the reach of the latest Drug Purchase and create a new
   * Exposure with the next DrugPurchase as the new Exposure.
   * This strategy is suited for short term effects.
   * !!! WARNING: THIS ONLY RETURNS EXPOSURES.
   *
   * @param minPurchases : Not used.
   * @param startDelay : period to be added to delay the start of each DrugPurchase.
   * @param purchasesWindow : Not used.
   * @param endThresholdGc : the period that defines the reach for Grand Conditionnement.
   * @param endThresholdNgc : the period that defines the reach for Non Grand Conditionnement.
   * @param endDelay : period added to the end of an exposure.
   * @return: A DataFrame of Exposures.
   */
  def withStartEnd(
    minPurchases: Int = 2,
    startDelay: Period = 5.days,
    purchasesWindow: Period = 4.months,
    endThresholdGc: Option[Period] = Some(120.days),
    endThresholdNgc: Option[Period] = Some(40.days),
    endDelay: Option[Period] = Some(0.months))
  : DataFrame = {

    val outputColumns = (data.columns.toList ++ List(ExposureStart, ExposureEnd)).map(col)

    val delayedDrugPurchases = delayStart(data, startDelay)

    val firstLastPurchase = getFirstAndLastPurchase(
      delayedDrugPurchases,
      endThresholdGc.get,
      endThresholdNgc.get,
      endDelay.get
    )

    toExposure(firstLastPurchase).select(outputColumns: _*)
  }

  def delayStart(data: DataFrame, startDelay: Period): DataFrame = {
    data.withColumn("NewStart", col("start").addPeriod(startDelay))
      .drop(col("start"))
      .withColumnRenamed("NewStart", "start")
  }

  def toExposure(firstLastPurchase: DataFrame): DataFrame = {
    val condition = (col("Status") === "first"
      && lead(col("Status"), 1).over(orderedWindow) === "last")
    firstLastPurchase.withColumn(
      ExposureEnd,
      when(condition, lead(col("purchaseReach"), 1).over(orderedWindow))
        .otherwise(col("purchaseReach"))
    )
      .where(col("Status") === "first")
      .withColumn(ExposureStart, col("start"))
      .drop("Status", "purchaseReach")
  }

  def getFirstAndLastPurchase(
    drugPurchases: DataFrame,
    endThresholdGc: Period,
    endThresholdNgc: Period,
    endDelay: Period): DataFrame = {
    val status = coalesce(
      when(col("previousPurchaseDate").isNull, "first"),
      when(col("previousPurchaseReach") < col(Start), "first"),
      when(col("purchaseReach") < col("nextPurchaseDate"), "last"),
      when(col("nextPurchaseDate").isNull, "last")
    )

    drugPurchases
      .withColumn("nextPurchaseDate", lead(col(Start), 1).over(orderedWindow))
      .withColumn("previousPurchaseDate", lag(col(Start), 1).over(orderedWindow))
      .withColumn(
        "purchaseReach",
        when(col("weight") === 1, col(Start).addPeriod(endThresholdGc).addPeriod(endDelay))
          .otherwise(col(Start).addPeriod(endThresholdNgc).addPeriod(endDelay))
      )
      .withColumn("previousPurchaseReach", lag(col("purchaseReach"), 1).over(orderedWindow))
      .withColumn("Status", status)
      .where(col("Status").isNotNull)
      .select((drugPurchases.columns.toList ++ List("Status", "purchaseReach")).map(col): _*)
  }
}
