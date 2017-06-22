package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl._
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.molecules.MoleculePurchases
import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig.{InputPaths, OutputPaths}
import fr.polytechnique.cmap.cnam.etl.patients.{Patient, Patients}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

object FilteringMain extends Main {

  val appName: String = "Filtering"

  /**
    * Arguments expected:
    *   "conf" -> "path/to/file.conf" (default: "$resources/filtering-default.conf")
    *   "env" -> "cnam" | "cmap" | "test" (deafult: "test")
    */
  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[FlatEvent]] = {

    import implicits.SourceReader
    import sqlContext.implicits._

    // "get" returns an Option, then we can use foreach to gently ignore when the key was not found.
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

  /* Alternative option using vars instead of SQLContext:
    argsMap.get("conf").foreach(FilteringConfig.setPath)
    argsMap.get("env").foreach(FilteringConfig.setEnv)
    FilteringConfig.init()
  */

    val reuseFlatEventsPath: Option[String] = FilteringConfig.reuseFlatEventsPath

    if (reuseFlatEventsPath.isDefined) {
      val flatEventsPath = reuseFlatEventsPath.get
      logger.info(s"Reusing flatEvents from $flatEventsPath")
      val flatEvents = sqlContext.read.parquet(flatEventsPath).as[FlatEvent]
      return Some(flatEvents)
    }

    val inputPaths: InputPaths = FilteringConfig.inputPaths
    val outputPaths: OutputPaths = FilteringConfig.outputPaths
    val cancerDefinition: String = FilteringConfig.cancerDefinition
    val extractionConfig: ExtractionConfig = ExtractionConfig.init()

    logger.info(s"Running for the $cancerDefinition cancer definition")

    logger.info("(Lazy) Extracting sources...")
    val sources: Sources = sqlContext.readSources(inputPaths)

    logger.info("(Lazy) Creating patients dataset...")
    val patients: Dataset[Patient] = Patients.extract(extractionConfig, sources).cache()

    logger.info("(Lazy) Creating drug events dataset...")
    val drugEvents: Dataset[Event] = MoleculePurchases.extract(extractionConfig, sources).map(Event.fromNewEvent(_))

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

    val flatEvents: Dataset[FlatEvent] = unionDatasets(drugFlatEvents, diseaseFlatEvents)

    logger.info("Writing Patients...")
    patients.toDF.write.parquet(outputPaths.patients)
    logger.info("Writing FlatEvents...")
    flatEvents.toDF.write.parquet(outputPaths.flatEvents)

    Some(flatEvents)
  }
}
