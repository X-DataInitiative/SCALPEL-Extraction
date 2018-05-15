package fr.polytechnique.cmap.cnam.study.rosiglitazone

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Diagnosis, Event, Molecule}
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
import fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes.RosiglitazoneOutcomeTransformer
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

object RosiglitazoneMain extends Main{

  val appName = "Rosiglitazone"

  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[Event[AnyEvent]]] = {

    import sqlContext.implicits._
    import EventFilters._
    import PatientFilters._

    val config = RosiglitazoneConfig.load(argsMap("conf"), argsMap("env"))
    val inputPaths = config.input
    val outputPaths = config.output
    logger.info("Input Paths: " + inputPaths.toString)
    logger.info("Output Paths: " + outputPaths.toString)
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

    logger.info("Extracting heart problems outcomes...")
    val outcomesTransformer = new RosiglitazoneOutcomeTransformer(config.outcomes.outcomeDefinition)
    val outcomes = outcomesTransformer.transform(diseaseEvents)

    logger.info("Extracting Follow-up...")
    val patientsWithObservations = patients.joinWith(observations, patients.col("patientId") === observations.col("patientId"))

    val followups = new FollowUpTransformer(config.followUp)
      .transform(patientsWithObservations, drugEvents, outcomes, tracklosses)
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

    logger.info("Writing heart problems outcomes...")
    outcomes.filterPatients(filteredPatients.idsSet)
      .write.parquet(outputPaths.outcomes)

    logger.info("Extracting Exposures...")
    val patientsWithFollowups = patients.joinWith(followups, followups.col("patientId") === patients.col("patientId"))

    val exposureConfig = ExposuresTransformerConfig()

    val exposures = new ExposuresTransformer(exposureConfig)
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

