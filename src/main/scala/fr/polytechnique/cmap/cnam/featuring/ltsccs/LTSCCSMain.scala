package fr.polytechnique.cmap.cnam.featuring.ltsccs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SQLContext}
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.old_root.FlatEvent

object LTSCCSMain extends Main {

  final val ExposureStartThresholds = List(3, 6)
  final val ExposureEndThresholds = List(3, 6)
  final val FilterPatients = List(false)

  override def appName = "LTSCCS"

  def runLTSCCS(sqlContext: SQLContext, config: Config): Unit = {
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
        flatEvents.union(ltsccsObservationPeriods), filterPatients
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

  override def main(args: Array[String]): Unit = {
    startContext()
    val environment = if (args.nonEmpty) args(0) else "test"
    val config: Config = ConfigFactory.parseResources("config/filtering-default.conf").getConfig(environment)
    runLTSCCS(sqlContext, config)
    stopContext()
  }

  // todo: refactor this function
  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = None
}
