package fr.polytechnique.cmap.cnam.study.fall.exposures

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposureDefinition, ExposurePeriodStrategy, WeightAggStrategy}
import me.danielpes.spark.datetime.implicits._

object FallStudyExposures {

  def fallMainExposuresDefinition(studyStart: Timestamp): ExposureDefinition = {

    ExposureDefinition(
      periodStrategy = ExposurePeriodStrategy.Limited,
      startDelay = 0.months,
      endDelay = 30.days,
      minPurchases = 1,
      purchasesWindow = 0.months,
      weightAggStrategy = WeightAggStrategy.NonCumulative,
      tracklossThreshold = 60.days,
      cumulativeExposureWindow = 0,
      cumulativeStartThreshold = 0,
      cumulativeEndThreshold = 0,
      dosageLevelIntervals = List(),
      purchaseIntervals = List(),
      studyStart = studyStart,
      filterDelayedPatients = false)
  }
}