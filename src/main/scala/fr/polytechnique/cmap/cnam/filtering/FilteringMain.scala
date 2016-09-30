package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.filtering.cox._
import fr.polytechnique.cmap.cnam.filtering.ltsccs._

object FilteringMain extends Main {

  def appName = "Filtering"

  def runETL(sqlContext: HiveContext, config: Config, cancerDefinition: String): Unit = {
    import implicits._
    import sqlContext.implicits._

    val outputRoot = config.getString("paths.output.root")
    val outputDir = s"$outputRoot/$cancerDefinition"

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
    val exposures = CoxExposuresTransformer.transform(flatEventsForExposures).cache()
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

    logger.info("Preparing for LTSCCS")
    val ltsccsObservationPeriods = LTSCCSObservationPeriodTransformer.transform(
      drugFlatEvents
        .union(tracklossFlatEvents)
    )
    val ltsccsDiseases = diseaseFlatEvents.toDF.where(col("start").isNotNull).as[FlatEvent]
    val ltsccsExposures = LTSCCSExposuresTransformer.transform(
      drugFlatEvents
        .union(ltsccsDiseases)
        .union(ltsccsObservationPeriods)
    )

    val coxPatients = exposures.map(_.patientID).distinct.persist()

    val ltsccsFlatEvents: Dataset[FlatEvent] =
      drugFlatEvents
        .union(ltsccsDiseases)
        .union(ltsccsExposures)
        .union(ltsccsObservationPeriods)
        .joinWith(coxPatients.as("p"), col("patientID") === col("p.value")).map{
          case (event: FlatEvent, p: String) => event
        }

    logger.info("Writing LTSCCS features...")
    import LTSCCSWriter._
    ltsccsFlatEvents.writeLTSCCS(s"$outputDir/LTSCCS")
  }

  def main(args: Array[String]): Unit = {
    startContext()
    val (environment, cancerDefinition) = if (args.nonEmpty)
      (args(0), args(1))
    else
      ("test", "broad")
    val config: Config = ConfigFactory.parseResources("filtering.conf").getConfig(environment)
    runETL(sqlContext, config, cancerDefinition)
    stopContext()
  }
}
