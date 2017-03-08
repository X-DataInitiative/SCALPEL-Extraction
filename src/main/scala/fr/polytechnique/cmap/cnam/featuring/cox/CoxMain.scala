package fr.polytechnique.cmap.cnam.featuring.cox

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl._
import fr.polytechnique.cmap.cnam.etl.exposures.ExposuresTransformer
import fr.polytechnique.cmap.cnam.etl.old_root._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources

/**
  * Created by sathiya on 09/11/16.
  */
object CoxMain extends Main {

  def appName = "CoxFeaturing"

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    logger.info("Running FilteringMain...")
    val flatEvents: Dataset[FlatEvent] = FilteringMain.run(sqlContext, argsMap).get
    coxFeaturing(flatEvents, argsMap)
  }

  def coxFeaturing(flatEvents: Dataset[FlatEvent], argsMap: Map[String, String]): Option[Dataset[_]] = {
    import flatEvents.sqlContext.implicits._

    val sqlContext = flatEvents.sqlContext

    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val cancerDefinition: String = FilteringConfig.cancerDefinition
    val outputRoot = FilteringConfig.outputPaths.coxFeatures
    val outputDir = s"$outputRoot/$cancerDefinition"

    val dcirFlat: DataFrame = sqlContext.read.parquet(FilteringConfig.inputPaths.dcir)

    val drugFlatEvents = flatEvents.filter(_.category == "molecule")
    val diseaseFlatEvents = flatEvents.filter(_.category == "disease")

    val patients: Dataset[Patient] = flatEvents
      .map(
        x => Patient(
          x.patientID,
          x.gender,
          x.birthDate,
          x.deathDate)
      ).distinct

    logger.info("Number of drug events: " + drugFlatEvents.count)
    logger.info("Caching disease events...")
    logger.info("Number of disease events: " + diseaseFlatEvents.count)

    logger.info("Preparing for Cox with the following parameters:")

    logger.info("(Lazy) Transforming Follow-up events...")
    val observationFlatEvents = CoxObservationPeriodTransformer.transform(drugFlatEvents).cache()

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

    val exposuresConfig = FilteringConfig.exposuresConfig
    val exposures = ExposuresTransformer(exposuresConfig).transform(flatEventsForExposures).cache()

    logger.info("Caching exposures...")
    logger.info("Number of exposures: " + exposures.count)

    logger.info("(Lazy) Transforming Cox features...")
    val coxFlatEvents = exposures.union(followUpFlatEvents)
    val coxFeatures = CoxTransformer.transform(coxFlatEvents)

    val flatEventsSummary = flatEventsForExposures
      .union(observationFlatEvents)
      .union(tracklossFlatEvents)

    logger.info("Writing summary of all cox events and config...")
    flatEventsSummary.toDF.write.parquet(s"$outputDir/eventsSummary")
    logger.info("Writing Exposures...")
    exposures.toDF.write.parquet(s"$outputDir/exposures")

    logger.info("Writing Cox features...")
    import CoxFeaturesWriter._
    coxFeatures.toDF.write.parquet(s"$outputDir/cox")
    coxFeatures.writeCSV(s"$outputDir/cox.csv")

    Some(coxFeatures)
  }
}