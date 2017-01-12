package fr.polytechnique.cmap.cnam.filtering.exposures

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.filtering.FilteringConfig
import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig

case class ExposuresConfig(
    studyStart: Timestamp,
    diseaseCode: String, // todo: ExposuresTransformer should not depend on diseases
    periodStrategy: ExposurePeriodStrategy,
    minPurchases: Int,
    purchasesWindow: Int,
    startDelay: Int,
    weightAggStrategy: WeightAggStrategy,
    filterDelayedPatients: Boolean,
    cumulativeExposureWindow: Int,
    cumulativeStartThreshold: Int,
    cumulativeEndThreshold: Int)

object ExposuresConfig {
  // todo: Remove filters from ExposuresConfig pipeline
  // This method is required for compatibility with current singleton-based configuration strategy
  // After calling this, the user can call .copy() to change some parameters
  // todo: change periodStrategy and weightAggStrategy to actually take from config file
//  def init(): ExposuresConfig = {
//    new ExposuresConfig(
//      studyStart = FilteringConfig.dates.studyStart,
//      diseaseCode = FilteringConfig.diseaseCode,
//      periodStrategy = CoxConfig.exposureDefinition.periodStrategy,
//      minPurchases = CoxConfig.exposureDefinition.minPurchases,
//      purchasesWindow = CoxConfig.exposureDefinition.purchasesWindow,
//      startDelay = CoxConfig.exposureDefinition.startDelay,
//      weightAggStrategy = CoxConfig.exposureDefinition.weightAggStrategy,
//      filterDelayedPatients = CoxConfig.filterDelayedPatients,
//      cumulativeExposureWindow = CoxConfig.exposureDefinition.cumulativeExposureWindow,
//      cumulativeStartThreshold = CoxConfig.exposureDefinition.cumulativeStartThreshold,
//      cumulativeEndThreshold = CoxConfig.exposureDefinition.cumulativeEndThreshold
//    )
//  }
  def init(): ExposuresConfig = FilteringConfig.exposuresConfig
}
