package fr.polytechnique.cmap.cnam.study.fall.exposures

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposureDefinition, ExposurePeriodStrategy, WeightAggStrategy}

object FallStudyExposures {

  def fallMainExposuresDefinition(studyStart: Timestamp): ExposureDefinition = {

    ExposureDefinition(
      periodStrategy = ExposurePeriodStrategy.Limited,
      startDelay = 0,
      endDelay = 1,
      minPurchases = 1,
      purchasesWindow = 0,
      weightAggStrategy = WeightAggStrategy.NonCumulative,
      tracklossThreshold = 2,
      cumulativeExposureWindow = 0,
      cumulativeStartThreshold = 0,
      cumulativeEndThreshold = 0,
      dosageLevelIntervals = List(),
      purchaseIntervals = List(),
      studyStart = studyStart,
      filterDelayedPatients = false)
  }
}