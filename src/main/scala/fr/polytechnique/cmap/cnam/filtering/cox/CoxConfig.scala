package fr.polytechnique.cmap.cnam.filtering.cox

import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.filtering.FilteringConfig

/**
  * Created by sathiya on 23/11/16.
  */
object CoxConfig {

  case class CoxExposureDefinition(
    minPurchases: Int,
    purchasesWindow: Int,
    startDelay: Int
  )

  private lazy val modelParams: Config = FilteringConfig.modelConfig("cox_parameters")

  lazy val filterDelayedPatients: Boolean = modelParams.getBoolean("filter_delayed_patients")
  lazy val delayedEntriesThreshold: Int = modelParams.getInt("delayed_entries_threshold")
  lazy val followUpMonthsDelay: Int = modelParams.getInt("follow_up_delay")

  lazy val exposureDefinition = CoxExposureDefinition(
    minPurchases = modelParams.getInt("exposures.min_purchases"),
    startDelay = modelParams.getInt("exposures.start_delay"),
    purchasesWindow = modelParams.getInt("exposures.purchases_window")
  )

  override def toString: String = {
    s"filterDelayedPatients -> $filterDelayedPatients \n" +
    s"delayedEntriesThreshold -> $delayedEntriesThreshold \n" +
    s"followUpMonthsDelay -> $followUpMonthsDelay \n" +
    s"exposureDefinition.minPurchases -> ${exposureDefinition.minPurchases} \n" +
    s"exposureDefinition.startDelay -> ${exposureDefinition.startDelay} \n" +
    s"exposureDefinition.purchasesWindow -> ${exposureDefinition.purchasesWindow}"
  }
}
