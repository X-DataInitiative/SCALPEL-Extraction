package fr.polytechnique.cmap.cnam.filtering.mlpp

import java.sql.Timestamp
import scala.collection.JavaConverters._
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.filtering.FilteringConfig
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS

object MLPPConfig {

  case class MLPPExposureDefinition (
    minPurchases: Int,
    startDelay: Int,
    purchasesWindow: Int,
    onlyFirst: Boolean,
    filterLostPatients: Boolean,
    filterDiagnosedPatients: Boolean,
    diagnosedPatientsThreshold: Int,
    filterDelayedEntries: Boolean,
    delayedEntryThreshold: Int
  )

  private lazy val conf: Config = FilteringConfig.modelConfig("mlpp_parameters")

  lazy val bucketSize: Int = conf.getInt("bucket_size")
  lazy val lagCount: Int = conf.getInt("lag_count")
  lazy val minTimestamp: Timestamp = makeTS(conf.getIntList("min_timestamp").asScala.toList)
  lazy val maxTimestamp: Timestamp = makeTS(conf.getIntList("max_timestamp").asScala.toList)

  lazy val exposureDefinition = MLPPExposureDefinition(
    minPurchases = conf.getInt("exposures.min_purchases"),
    startDelay = conf.getInt("exposures.start_delay"),
    purchasesWindow = conf.getInt("exposures.purchases_window"),
    onlyFirst = conf.getBoolean("exposures.only_first"),
    filterLostPatients = conf.getBoolean("exposures.filter_lost_patients"),
    filterDiagnosedPatients = conf.getBoolean("exposures.filter_diagnosed_patients"),
    diagnosedPatientsThreshold = conf.getInt("exposures.diagnosed_patients_threshold"),
    filterDelayedEntries = conf.getBoolean("exposures.filter_delayed_entries"),
    delayedEntryThreshold = conf.getInt("exposures.delayed_entry_threshold")
  )
}
