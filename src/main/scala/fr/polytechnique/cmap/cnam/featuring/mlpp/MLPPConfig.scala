package fr.polytechnique.cmap.cnam.featuring.mlpp

import java.sql.Timestamp
import scala.collection.JavaConverters._
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig
import fr.polytechnique.cmap.cnam.util.functions.makeTS

object MLPPConfig {

  case class MLPPExposureDefinition (
    minPurchases: Int,
    startDelay: Int,
    purchasesWindow: Int,
    onlyFirst: Boolean,
    filterNeverSickPatients: Boolean,
    filterLostPatients: Boolean,
    filterEarlyDiagnosedPatients: Boolean,
    diagnosedPatientsThreshold: Int,
    filterDelayedEntries: Boolean,
    delayedEntryThreshold: Int
  )

  private lazy val conf: Config = FilteringConfig.modelConfig("mlpp_parameters")

  lazy val bucketSizes: List[Int] = conf.getIntList("bucket_size").asScala.toList.map(_.toInt)
  lazy val lagCounts: List[Int] = conf.getIntList("lag_count").asScala.toList.map(_.toInt)
  lazy val minTimestamp: Timestamp = makeTS(conf.getIntList("min_timestamp").asScala.toList)
  lazy val maxTimestamp: Timestamp = makeTS(conf.getIntList("max_timestamp").asScala.toList)
  lazy val includeDeathBucket: Boolean = conf.getBoolean("include_death_bucket")
  lazy val featuresAsList: Boolean = conf.getBoolean("features_as_list")

  lazy val exposureDefinition = MLPPExposureDefinition(
    minPurchases = conf.getInt("exposures.min_purchases"),
    startDelay = conf.getInt("exposures.start_delay"),
    purchasesWindow = conf.getInt("exposures.purchases_window"),
    onlyFirst = conf.getBoolean("exposures.only_first"),
    filterNeverSickPatients = conf.getBoolean("exposures.filter_never_sick_patients"),
    filterLostPatients = conf.getBoolean("exposures.filter_lost_patients"),
    filterEarlyDiagnosedPatients = conf.getBoolean("exposures.filter_diagnosed_patients"),
    diagnosedPatientsThreshold = conf.getInt("exposures.diagnosed_patients_threshold"),
    filterDelayedEntries = conf.getBoolean("exposures.filter_delayed_entries"),
    delayedEntryThreshold = conf.getInt("exposures.delayed_entry_threshold")
  )
}
