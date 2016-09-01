package fr.polytechnique.cmap.cnam.filtering

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.Main
import org.apache.log4j.Level

object FilteringMain extends Main {

  def appName = "Filtering"

  def runETL(sqlContext: HiveContext, config: Config): Unit = {
    import sqlContext.implicits._
    import implicits._
    logger.info("(Lazy) Extracting sources...")
    val sources: Sources = sqlContext.extractAll(config.getConfig("paths.input"))

    logger.info("(Lazy) Transforming patients...")
    val patients: Dataset[Patient] = PatientsTransformer.transform(sources)
    logger.info("(Lazy) Transforming drug events...")
    val drugEvents: Dataset[Event] = DrugEventsTransformer.transform(sources)
    logger.info("(Lazy) Transforming disease events...")
    val diseaseEvents: Dataset[Event] = DiseaseTransformer.transform(sources)

    val events = drugEvents.union(diseaseEvents)
    val flatEvents: Dataset[FlatEvent] = events.as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)

    logger.info("(Lazy) Transforming exposures...")
    val exposures = ExposuresTransformer.transform(flatEvents)


    logger.info("Writing Patients...")
    patients.toDF.write.parquet(config.getString("paths.output.patients"))
    logger.info("Writing FlatEvents...")
    flatEvents.toDF.write.parquet(config.getString("paths.output.events"))
    logger.info("Writing Exposures...")
    exposures.toDF.write.parquet(config.getString("paths.output.exposures"))
  }

  def main(args: Array[String]): Unit = {
    initContexts()
    val environment = if (args.nonEmpty) args(0) else "test"
    val config: Config = ConfigFactory.parseResources("filtering.conf").getConfig(environment)
    runETL(sqlContext, config)
  }
}
