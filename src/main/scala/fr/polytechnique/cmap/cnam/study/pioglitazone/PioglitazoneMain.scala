package fr.polytechnique.cmap.cnam.study.pioglitazone

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActs
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.Diagnoses
import fr.polytechnique.cmap.cnam.etl.extractors.molecules.MoleculePurchases
import fr.polytechnique.cmap.cnam.etl.extractors.patients.Patients
import fr.polytechnique.cmap.cnam.etl.extractors.tracklosses.{Tracklosses, TracklossesConfig}
import fr.polytechnique.cmap.cnam.etl.filters.{EventFilters, PatientFilters}
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.MLPPLoader
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposuresTransformer, ExposuresTransformerConfig}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformer
import fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes._
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions._


object PioglitazoneMain extends Main {

  val appName: String = "Pioglitazone"

  /**
    * Arguments expected:
    *   "conf" -> "path/to/file.conf" (default: "$resources/config/pioglitazone/default.conf")
    *   "env" -> "cnam" | "cmap" | "test" (default: "test")
    */
  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[Event[AnyEvent]]] = {

    import sqlContext.implicits._
    import EventFilters._
    import PatientFilters._

    val config = PioglitazoneConfig.load(argsMap("conf"), argsMap("env"))
    val inputPaths = config.input
    val outputPaths = config.output
    logger.info("Input Paths: " + inputPaths.toString)
    logger.info("Output Paths: " + outputPaths.toString)
    logger.info("study config....")
    logger.info("===================================")

    logger.info("Reading sources")
    import implicits.SourceReader
    val sources: Sources = Sources.sanitize(sqlContext.readSources(inputPaths))

    logger.info("Extracting patients...")
    val patients: Dataset[Patient] = new Patients(config.patients).extract(sources).cache()

    logger.info("Extracting molecule events...")
    val drugEvents: Dataset[Event[Molecule]] = new MoleculePurchases(config.molecules).extract(sources).cache()

    logger.info("Extracting diagnosis events...")
    val diseaseEvents: Dataset[Event[Diagnosis]] = new Diagnoses(config.diagnoses).extract(sources).cache()

    logger.info("Extracting medical acts...")
    val medicalActs = new MedicalActs(config.medicalActs).extract(sources)

    logger.info("Merging all events...")
    val allEvents: Dataset[Event[AnyEvent]] = unionDatasets(
      drugEvents.as[Event[AnyEvent]],
      diseaseEvents.as[Event[AnyEvent]]
    )

    logger.info("Extracting Tracklosses...")
    val tracklossConfig = TracklossesConfig(studyEnd = config.base.studyEnd)
    val tracklosses = new Tracklosses(tracklossConfig).extract(sources).cache()

    logger.info("Writing patients...")
    patients.toDF.write.parquet(outputPaths.patients)

    logger.info("Writing events...")
    allEvents.toDF.write.parquet(outputPaths.flatEvents)

    logger.info("Extracting Observations...")
    val observations = new ObservationPeriodTransformer(config.observationPeriod).transform(allEvents).cache()

    logger.info("Extracting cancer outcomes...")
    val outcomesTransformer = new PioglitazoneOutcomeTransformer(config.outcomes.cancerDefinition)
    val outcomes = outcomesTransformer.transform(diseaseEvents, medicalActs)

    logger.info("Extracting Follow-up...")
    val patiensWithObservations = patients.joinWith(observations, patients.col("patientId") === observations.col("patientId"))

    val followups = new FollowUpTransformer(config.followUp)
      .transform(patiensWithObservations, drugEvents, outcomes, tracklosses)
      .cache()

    logger.info("Filtering Patients...")
    val filteredPatients = {
      val firstFilterResult = if (config.filters.filterDelayedEntries)
        patients.filterDelayedPatients(drugEvents, config.base.studyStart, config.filters.delayedEntryThreshold)
      else
        patients

      if (config.filters.filterDiagnosedPatients)
        firstFilterResult.filterEarlyDiagnosedPatients(outcomes, followups, outcomesTransformer.outcomeName)
      else
        patients
    }

    logger.info("Writing cancer outcomes...")
    outcomes.filterPatients(filteredPatients.idsSet)

    logger.info("Extracting Exposures...")
    val patientsWithFollowups = filteredPatients.joinWith(
      followups, followups.col("patientId") === patients.col("patientId"))

    val exposuresConfig = ExposuresTransformerConfig()

    val exposures = new ExposuresTransformer(exposuresConfig)
      .transform(patientsWithFollowups, drugEvents)
      .cache()

    logger.info("Writing Exposures...")
    exposures.write.parquet(outputPaths.exposures)

    logger.info("Extracting MLPP features...")
    val params = MLPPLoader.Params(minTimestamp = config.base.studyStart, maxTimestamp = config.base.studyEnd)
    MLPPLoader(params).load(outcomes, exposures, patients, outputPaths.mlppFeatures)

    Some(allEvents)
  }
}