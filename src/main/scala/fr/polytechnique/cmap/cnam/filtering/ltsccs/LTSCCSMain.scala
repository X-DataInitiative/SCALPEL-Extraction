package fr.polytechnique.cmap.cnam.filtering.ltsccs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.filtering.FlatEvent

object LTSCCSMain extends Main {

  final val ExposureStartThresholds = List(3, 6)
  final val ExposureEndThresholds = List(3, 6)
  final val FilterPatients = List(false)

  override def appName = "LTSCCS"

  def runLTSCCS(sqlContext: HiveContext, config: Config): Unit = {
    import sqlContext.implicits._

    val coxPatients = sqlContext.read.parquet(config.getString("paths.output.exposures"))
      .as[FlatEvent]
      .map(_.patientID)
      .distinct
      .persist()
    val flatEvents = sqlContext.read.parquet(config.getString("paths.output.events"))
      .as[FlatEvent]
      .persist()
    val outRootDir = config.getString("paths.output.LTSCCSFeatures")

    val ltsccsObservationPeriods = LTSCCSObservationPeriodTransformer.transform(flatEvents)

    for {
      startThreshold <- ExposureStartThresholds
      endThreshold <- ExposureEndThresholds
      filterPatients <- FilterPatients
    } {
      val dir = s"$outRootDir/cross-validation/start_$startThreshold/end_$endThreshold/filter_$filterPatients"

      val ltsccsExposures = LTSCCSExposuresTransformer.transform(
        flatEvents.union(ltsccsObservationPeriods)//, filterPatients // We need to merge branch CNAM-117
      )

      val ltsccsFinalEvents = flatEvents.union(ltsccsObservationPeriods).union(ltsccsExposures).as("e")
        .joinWith(coxPatients.as("p"), col("e.patientID") === col("p.value"))
        .map{
          case (event: FlatEvent, p: String) => event
        }

      logger.info(s"Writing LTSCCS features in $dir")
      import LTSCCSWriter._
      ltsccsFinalEvents.writeLTSCCS(dir)
    }

  }

  def main(args: Array[String]): Unit = {
    startContext()
    val environment = if (args.nonEmpty) args(0) else "test"
    val config: Config = ConfigFactory.parseResources("filtering.conf").getConfig(environment)
    runLTSCCS(sqlContext, config)
    stopContext()
  }
}
