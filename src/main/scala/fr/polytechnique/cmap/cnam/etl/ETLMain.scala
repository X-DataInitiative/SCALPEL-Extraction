package fr.polytechnique.cmap.cnam.etl

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extract.diagnoses.Diagnoses
import fr.polytechnique.cmap.cnam.etl.extract.molecules.MoleculePurchases
import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig
import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig.{InputPaths, OutputPaths}
import fr.polytechnique.cmap.cnam.etl.patients.{Patient, Patients}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.pioglitazone.NaiveBladderCancer
import fr.polytechnique.cmap.cnam.util.functions._

object ETLMain extends Main {

  val appName: String = "Filtering"

  /**
    * Arguments expected:
    *   "conf" -> "path/to/file.conf" (default: "$resources/filtering-default.conf")
    *   "env" -> "cnam" | "cmap" | "test" (default: "test")
    */
  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[Event[AnyEvent]]] = {

    import sqlContext.implicits._

    // "get" returns an Option, then we can use foreach to gently ignore when the key was not found.
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    // todo rename the config variable
    val reuseLastETLPath: Option[String] = FilteringConfig.reuseFlatEventsPath

    if (reuseLastETLPath.isDefined) {
      val eventsPath = reuseLastETLPath.get
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
    import implicits.SourceReader
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

    logger.info("Extracting bladder cancer outcomes...")
    val naiveBladderCancerOutcomes = NaiveBladderCancer.transform(diseaseEvents)

    logger.info("Writing bladder cancer outcomes...")
    naiveBladderCancerOutcomes.toDF.write.parquet(outputPaths.NaiveBladderCancerOutcomes)

    Some(allEvents)
  }
}
