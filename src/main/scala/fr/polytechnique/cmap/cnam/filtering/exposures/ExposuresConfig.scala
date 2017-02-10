package fr.polytechnique.cmap.cnam.filtering.exposures

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.filtering.FilteringConfig

// todo: Remove filters from ExposuresConfig pipeline
case class ExposuresConfig(
    studyStart: Timestamp,
    diseaseCode: String, // todo: ExposuresTransformer should not depend on diseases
    periodStrategy: ExposurePeriodStrategy,
    followUpDelay: Int,
    minPurchases: Int,
    purchasesWindow: Int,
    startDelay: Int,
    weightAggStrategy: WeightAggStrategy,
    filterDelayedPatients: Boolean,
    cumulativeExposureWindow: Int,
    cumulativeStartThreshold: Int,
    cumulativeEndThreshold: Int,
    dosageLevelIntervals: List[Int],
    purchaseIntervals: List[Int])

object ExposuresConfig {
  def init(): ExposuresConfig = FilteringConfig.exposuresConfig
}
