package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events.{Dispensation, Event, Exposure}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers._
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._

/**
  * A class to represent the exposure we want to generate from the data
  *
  * periodStrategy            Period strategy. Possible values: "unlimited" | "limited"
  * (multiple exposures with start and end)
  *
  * startDelay                Number of periods after which a patient will be considered exposed
  * after the min purchases window.
  *
  * endDelay                  Number of periods that we add to the exposure end to delay it (lag)
  *
  * minPurchases              Minimum number of purchases that have to be made in order to be considered exposed.
  *
  * purchasesWindow           Purchase window, period within which the min number of purchases have to be made.
  *
  * weightAggStrategy         Weight Aggregation strategy.
  * Possible values: "non-cumulative" |  "purchase-based" | "dosage-based" | "time-based"
  *
  * endThreshold              If periodStrategy="limited", represents the period without purchases
  * for an exposure to be considered "finished"
  *
  * cumulativeExposureWindow  Number of months to quantile.
  *
  * cumulativeStartThreshold  Number of months within which more than one purchases have to made
  *
  * cumulativeEndThreshold    Number of months during which no purchases of the particular molecule have to be made
  *
  * dosageLevelIntervals      List of consumption levels in mg / put only 0 when we want all the weights to 1
  *
  * purchaseIntervals         List of consumption levels in mg / put only 0 when we want all the weights to 1
  */
class ExposuresTransformerConfig[Disp <: Dispensation](
  val patients: Option[Dataset[(Patient, Event[FollowUp])]] = None,
  val dispensations: Option[Dataset[Event[Disp]]] = None,
  val startDelay: Period,
  val minPurchases: Int,
  val purchasesWindow: Period,

  // Idea for the future: the following blocks could evolve into two ADTs (pureconfig supports it)
  val periodStrategy: ExposurePeriodStrategy,
  val endThresholdGc: Option[Period],
  val endThresholdNgc: Option[Period],
  val endDelay: Option[Period],

  val weightAggStrategy: WeightAggStrategy,
  val cumulativeExposureWindow: Option[Int],
  val cumulativeStartThreshold: Option[Int],
  val cumulativeEndThreshold: Option[Int],
  val dosageLevelIntervals: Option[List[Int]],
  val purchaseIntervals: Option[List[Int]]) extends TransformerConfig

object ExposuresTransformerConfig{

  def apply[Disp <: Dispensation] (
      patients: Option[Dataset[(Patient, Event[FollowUp])]] = None,
      dispensations: Option[Dataset[Event[Disp]]] = None,
      startDelay: Period = 3.months,
      minPurchases: Int = 2,
      purchasesWindow: Period = 6.months,
      periodStrategy: ExposurePeriodStrategy = ExposurePeriodStrategy.Unlimited,
      endThresholdGc: Option[Period] = Some(3.months),
      endThresholdNgc: Option[Period] = Some(1.months),
      endDelay: Option[Period] = Some(0.months),
      weightAggStrategy: WeightAggStrategy = WeightAggStrategy.NonCumulative,
      cumulativeExposureWindow: Option[Int] = Some(1),
      cumulativeStartThreshold: Option[Int] = Some(6),
      cumulativeEndThreshold: Option[Int] = Some(4),
      dosageLevelIntervals: Option[List[Int]] = Some(List(0, 100, 200, 300, 400, 500)),
      purchaseIntervals: Option[List[Int]] = Some(List(0, 3, 5)))
    : ExposuresTransformerConfig[Disp] = {

    new ExposuresTransformerConfig[Disp] (
      patients,
      dispensations,

      startDelay,
      minPurchases,
      purchasesWindow,

      periodStrategy,
      endThresholdGc,
      endThresholdNgc,
      endDelay,

      weightAggStrategy,
      cumulativeExposureWindow,
      cumulativeStartThreshold,
      cumulativeEndThreshold,
      dosageLevelIntervals,
      purchaseIntervals
    )
  }
}
