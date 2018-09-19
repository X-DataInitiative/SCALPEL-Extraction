package fr.polytechnique.cmap.cnam.study.fall.exposures

import java.sql.Timestamp
import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposurePeriodStrategy, ExposuresTransformerConfig, WeightAggStrategy}

object FallStudyExposures {

  def fallMainExposuresDefinition(studyStart: Timestamp): ExposuresTransformerConfig = {

    ExposuresTransformerConfig(
      startDelay = 0.months,
      minPurchases = 1,
      purchasesWindow = 0.months,

      periodStrategy = ExposurePeriodStrategy.Limited,
      endThresholdGc = Some(90.days),
      endThresholdNgc = Some(30.days),
      endDelay = Some(30.days),

      weightAggStrategy = WeightAggStrategy.NonCumulative,
      cumulativeExposureWindow = Some(0),
      cumulativeStartThreshold = Some(0),
      cumulativeEndThreshold = Some(0),
      dosageLevelIntervals = Some(List()),
      purchaseIntervals = Some(List()))
  }
}