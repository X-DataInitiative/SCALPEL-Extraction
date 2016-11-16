package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.utilities.functions._

object FilteringMain extends Main {

  def appName = "Filtering"

  def run(sqlContext: HiveContext, args: Array[String]): Unit = {

    import implicits.SourceExtractor
    import sqlContext.implicits._

    sqlContext.setConf("config_path", args(0))
    sqlContext.setConf("environment", args(1))

    val inputPaths = FilteringConfig.inputPaths
    val outputPaths = FilteringConfig.outputPaths
    val cancerDefinition = FilteringConfig.cancerDefinition

    logger.info("(Lazy) Extracting sources...")
    val sources: Sources = sqlContext.extractAll(inputPaths)

    logger.info("(Lazy) Creating patients dataset...")
    val patients: Dataset[Patient] = PatientsTransformer.transform(sources).cache()

    logger.info("(Lazy) Creating drug events dataset...")
    val drugEvents: Dataset[Event] = DrugEventsTransformer.transform(sources)

    logger.info("(Lazy) Creating disease events dataset...")
    val broadDiseaseEvents: Dataset[Event] = DiseaseTransformer.transform(sources)
    val targetDiseaseEvents: Dataset[Event] = (
      if (cancerDefinition == "broad")
        broadDiseaseEvents
      else
        TargetDiseaseTransformer.transform(sources)
    ).map(_.copy(eventId = "targetDisease"))
    val diseaseEvents: Dataset[Event] = broadDiseaseEvents.union(targetDiseaseEvents)

    logger.info("(Lazy) Creating flat events")
    val drugFlatEvents: Dataset[FlatEvent] = drugEvents.as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)

    val diseaseFlatEvents: Dataset[FlatEvent] = diseaseEvents.as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)

    val flatEvents: Dataset[FlatEvent] = unionAll(drugFlatEvents, diseaseFlatEvents)

    logger.info("Writing Patients...")
    patients.toDF.write.parquet(outputPaths.patients)
    logger.info("Writing FlatEvents...")
    flatEvents.toDF.write.parquet(outputPaths.flatEvents)
  }
}
