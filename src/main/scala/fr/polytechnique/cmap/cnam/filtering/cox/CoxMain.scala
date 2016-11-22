package fr.polytechnique.cmap.cnam.filtering.cox

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.filtering._

/**
  * Created by sathiya on 09/11/16.
  */
object CoxMain extends Main {

  def appName = "CoxFeaturing"

  def coxFeaturing(sqlContext: HiveContext,
                   config: Config,
                   cancerDefinition: String,
                   filterDelayedPatients: Boolean): Unit = {
    import sqlContext.implicits._

    val flatEventPath = config.getString("paths.input.flatEvent")
    val flatDcirPath = config.getString("paths.input.flatDcir")
    val outputRoot = config.getString("paths.output.root")
    val outputDir = s"$outputRoot/$cancerDefinition/$filterDelayedPatients"

    logger.info(s"Reading flat events from $flatEventPath...")

    val dcirFlat: DataFrame = sqlContext.read.parquet(flatDcirPath)
    val flatEvents: DataFrame = sqlContext.read.parquet(flatEventPath)

    val drugFlatEvents = flatEvents.filter(col("category") === "molecule").as[FlatEvent]
    val diseaseFlatEvents = flatEvents.filter(col("category") === "disease").as[FlatEvent]
    val patientColumns = Array($"patientID", $"gender", $"birthDate", $"deathDate")
    val patients = flatEvents.select(patientColumns: _*).distinct.as[Patient]

    logger.info("Number of drug events: " + drugFlatEvents.count)
    logger.info("Caching disease events...")
    logger.info("Number of disease events: " + diseaseFlatEvents.count)

    logger.info("Preparing for Cox")
    logger.info("(Lazy) Transforming Follow-up events...")
    val observationFlatEvents = CoxObservationPeriodTransformer.transform(drugFlatEvents)

    val tracklossEvents: Dataset[Event] = TrackLossTransformer.transform(Sources(dcir=Some(dcirFlat)))
    val tracklossFlatEvents = tracklossEvents
      .as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)
      .cache()

    val followUpFlatEvents = CoxFollowUpEventsTransformer.transform(
      drugFlatEvents
        .union(diseaseFlatEvents)
        .union(observationFlatEvents)
        .union(tracklossFlatEvents)
    ).cache()

    logger.info("(Lazy) Transforming exposures...")
    val flatEventsForExposures =
      drugFlatEvents
        .union(diseaseFlatEvents)
        .union(followUpFlatEvents)
    val exposures = CoxExposuresTransformer.transform(flatEventsForExposures,
      filterDelayedPatients).cache()

    logger.info("Caching exposures...")
    logger.info("Number of exposures: " + exposures.count)

    logger.info("(Lazy) Transforming Cox features...")
    val coxFlatEvents = exposures.union(followUpFlatEvents)
    val coxFeatures = CoxTransformer.transform(coxFlatEvents)

    val flatEventsSummary = flatEventsForExposures
      .union(observationFlatEvents)
      .union(tracklossFlatEvents)

    logger.info("Writing summary of all cox events...")
    flatEventsSummary.toDF.write.parquet(s"$outputDir/eventsSummary")
    logger.info("Writing Exposures...")
    exposures.toDF.write.parquet(s"$outputDir/exposures")

    logger.info("Writing Cox features...")
    import CoxFeaturesWriter._
    coxFeatures.toDF.write.parquet(s"$outputDir/cox")
    coxFeatures.writeCSV(s"$outputDir/cox.csv")
  }

  override def main(args: Array[String]): Unit = {
    startContext()
    val (environment: String, cancerDefinition: String, filterDelayedPatients: Boolean) =
      args match {
        case Array(arg1, args2, args3) => (args(0), args(1), args(2).toBoolean)
        case Array(arg1, args2) => (args(0), args(1), true)
        case _ => ("test", "broad", true)
      }
    val config: Config = ConfigFactory.parseResources("filtering-default.conf").getConfig(environment)
    coxFeaturing(sqlContext, config, cancerDefinition, filterDelayedPatients)
    stopContext()
  }

  // todo: refactor this function
  def run(sqlContext: HiveContext, argsMap: Map[String, String]): Option[Dataset[_]] = None
}