package fr.polytechnique.cmap.cnam.etl

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.{Diagnoses, Diagnosis}
import fr.polytechnique.cmap.cnam.etl.events.molecules.{Molecule, MoleculePurchases}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig
import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig.{InputPaths, OutputPaths}
import fr.polytechnique.cmap.cnam.etl.patients.{Patient, Patients}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

object ETLMain extends Main {

  val appName: String = "Filtering"

  /**
    * Arguments expected:
    *   "conf" -> "path/to/file.conf" (default: "$resources/filtering-default.conf")
    *   "env" -> "cnam" | "cmap" | "test" (deafult: "test")
    */
  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[Event[AnyEvent]]] = {

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
      val eventsPath = reuseFlatEventsPath.get
      logger.info(s"Reusing events from $eventsPath")
      val events = sqlContext.read.parquet(eventsPath).as[Event[AnyEvent]]
      return Some(events)
    }

    val inputPaths: InputPaths = FilteringConfig.inputPaths
    val outputPaths: OutputPaths = FilteringConfig.outputPaths
    val extractionConfig: ExtractionConfig = ExtractionConfig.init()

    logger.info("Input Paths: " + inputPaths.toString)
    logger.info("Output Paths: " + outputPaths.toString)
    logger.info("Extraction Config: " + extractionConfig.toString)
    logger.info("===================================")

    logger.info("Reading sources")
    val sources: Sources = sqlContext.readSources(inputPaths)

    logger.info("Extracting patients...")
    val patients: Dataset[Patient] = Patients.extract(extractionConfig, sources).cache()

    logger.info("Extracting molecule events...")
    val drugEvents: Dataset[Event[Molecule]] = MoleculePurchases.extract(extractionConfig, sources)

    logger.info("Extracting diagnosis events...")
    val diseaseEvents: Dataset[Event[Diagnosis]] = Diagnoses.extract(extractionConfig, sources)

    logger.info("Merging all events...")
    val allEvents: Dataset[Event[AnyEvent]] = unionDatasets(
      drugEvents.as[Event[AnyEvent]],
      diseaseEvents.as[Event[AnyEvent]]
    )

    logger.info("Writing patients...")
    patients.toDF.write.parquet(outputPaths.patients)

    logger.info("Writing events...")
    allEvents.toDF.write.parquet(outputPaths.flatEvents)

    Some(allEvents)
  }
}
