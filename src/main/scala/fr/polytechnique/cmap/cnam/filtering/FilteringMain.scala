package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.utilities.functions._

object FilteringMain extends Main {

  def appName = "Filtering"

  /**
    * Arguments expected:
    *   "conf" -> "path/to/file.conf" (default: "$resources/filtering-default.conf")
    *   "env" -> "cnam" | "cmap" | "test" (deafult: "test")
    */
  override def run(sqlContext: HiveContext, argsMap: Map[String, String] = Map()): Unit = {

    import implicits.SourceExtractor
    import sqlContext.implicits._

    // "get" returns an Option, then we can use foreach to gently ignore when the key was not found.
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

  /* Alternative option using vars instead of SQLContext:
    argsMap.get("conf").foreach(FilteringConfig.setPath)
    argsMap.get("env").foreach(FilteringConfig.setEnv)
    FilteringConfig.init()
  */

    val inputPaths = FilteringConfig.inputPaths
    val outputPaths = FilteringConfig.outputPaths
    val cancerDefinition = FilteringConfig.cancerDefinition

    logger.info(s"Running for the $cancerDefinition cancer definition")

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
