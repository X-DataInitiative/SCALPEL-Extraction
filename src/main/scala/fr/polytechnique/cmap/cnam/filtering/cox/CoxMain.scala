package fr.polytechnique.cmap.cnam.filtering.cox

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.filtering._

/**
  * Created by sathiya on 09/11/16.
  */
//TODO: Avoid redoing the extraction and transformation, instead reuse "hdfs://shared/filtered_data"
object CoxMain extends Main {

  def appName = "CoxFeaturing"

  def coxFeaturing(sqlContext: HiveContext,
                   config: Config,
                   cancerDefinition: String,
                   filterDelayedPatients: Boolean): Unit = {
    import implicits._
    import sqlContext.implicits._

    val outputRoot = config.getString("paths.output.root")
    val outputDir = s"$outputRoot/$cancerDefinition/$filterDelayedPatients"

    logger.info("(Lazy) Extracting sources...")
    val sources: Sources = sqlContext.extractAll(config.getConfig("paths.input"))

    logger.info("(Lazy) Transforming patients...")
    val patients: Dataset[Patient] = PatientsTransformer.transform(sources).cache()

    logger.info("(Lazy) Transforming drug events...")
    val drugEvents: Dataset[Event] = DrugEventsTransformer.transform(sources)

    logger.info("(Lazy) Transforming disease events...")
    val broadDiseaseEvents: Dataset[Event] = DiseaseTransformer.transform(sources)
    val targetDiseaseEvents: Dataset[Event] = (
      if (cancerDefinition == "broad")
        broadDiseaseEvents
      else
        TargetDiseaseTransformer.transform(sources)
      ).map(_.copy(eventId = "targetDisease"))
    val diseaseEvents = broadDiseaseEvents.union(targetDiseaseEvents)

    val drugFlatEvents = drugEvents.as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)
      .cache()

    val diseaseFlatEvents = diseaseEvents.as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)
      .cache()

    logger.info("Caching drug events...")
    logger.info("Number of drug events: " + drugFlatEvents.count)
    logger.info("Caching disease events...")
    logger.info("Number of disease events: " + diseaseFlatEvents.count)

    logger.info("Preparing for Cox")
    logger.info("(Lazy) Transforming Follow-up events...")
    val observationFlatEvents = CoxObservationPeriodTransformer.transform(drugFlatEvents)

    val tracklossEvents: Dataset[Event] = TrackLossTransformer.transform(sources)
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

    val flatEvents = flatEventsForExposures
      .union(observationFlatEvents)
      .union(tracklossFlatEvents)

    logger.info("Writing Patients...")
    patients.toDF.write.parquet(s"$outputDir/patients")
    logger.info("Writing FlatEvents...")
    flatEvents.toDF.write.parquet(s"$outputDir/events")
    logger.info("Writing Exposures...")
    exposures.toDF.write.parquet(s"$outputDir/exposures")

    logger.info("Writing Cox features...")
    import CoxFeaturesWriter._
    coxFeatures.toDF.write.parquet(s"$outputDir/cox")
    coxFeatures.writeCSV(s"$outputDir/cox.csv")
  }

  def main(args: Array[String]): Unit = {
    startContext()
    val (environment: String, cancerDefinition: String, filterDelayedPatients: Boolean) =
      args match {
        case Array(arg1, args2, args3) => (args(0), args(1), args(2).toBoolean)
        case Array(arg1, args2) => (args(0), args(1), true)
        case _ => ("test", "broad", true)
      }
    val config: Config = ConfigFactory.parseResources("filtering.conf").getConfig(environment)
    coxFeaturing(sqlContext, config, cancerDefinition, filterDelayedPatients)
    stopContext()
  }
}