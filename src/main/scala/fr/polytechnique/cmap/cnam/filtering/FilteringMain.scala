package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main

object FilteringMain extends Main {

  def appName = "Filtering"

  def runETL(sqlContext: HiveContext, config: Config): Unit = {
    import implicits._
    import sqlContext.implicits._

    logger.info("(Lazy) Extracting sources...")
    val sources: Sources = sqlContext.extractAll(config.getConfig("paths.input"))

    logger.info("(Lazy) Transforming patients...")
    val patients: Dataset[Patient] = PatientsTransformer.transform(sources).cache()
    logger.info("(Lazy) Transforming drug events...")
    val drugEvents: Dataset[Event] = DrugEventsTransformer.transform(sources)
    logger.info("(Lazy) Transforming disease events...")
    val diseaseEvents: Dataset[Event] = DiseaseTransformer.transform(sources)
    logger.info("(Lazy) Transforming trackloss events...")
    val tracklossEvents: Dataset[Event] = TrackLossTransformer.transform(sources)

    val drugFlatEvents = drugEvents.as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)
      .cache()

    val diseaseFlatEvents = diseaseEvents.as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)
      .cache()

    val tracklossFlatEvents = tracklossEvents.as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)
      .cache()

    logger.info("Caching drug events...")
    logger.info("Number of drug events: " + drugFlatEvents.count)
    logger.info("Caching disease events...")
    logger.info("Number of disease events: " + diseaseFlatEvents.count)

    val flatEvents = drugFlatEvents
      .union(diseaseFlatEvents)
      .union(tracklossFlatEvents)

    logger.info("(Lazy) Transforming exposures...")
    val exposures = ExposuresTransformer.transform(flatEvents).cache()
    logger.info("Caching exposures...")
    logger.info("Number of exposures: " + exposures.count)

    // Todo: union exposures with flatEvents, then use the resulting Dataset as input to the model transformers
    // Example:
    //   val finalEvents = flatEvents.union(exposures)
    //   CoxphTransformer.transform(finalEvents).writeCoxphInput

    logger.info("Writing Patients...")
    patients.toDF.write.parquet(config.getString("paths.output.patients"))
    logger.info("Writing FlatEvents...")
    flatEvents.toDF.write.parquet(config.getString("paths.output.events"))
    logger.info("Writing Exposures...")
    exposures.toDF.write.parquet(config.getString("paths.output.exposures"))
  }

  def main(args: Array[String]): Unit = {
    startContext()
    val environment = if (args.nonEmpty) args(0) else "test"
    val config: Config = ConfigFactory.parseResources("filtering.conf").getConfig(environment)
    runETL(sqlContext, config)
    stopContext()
  }
}
