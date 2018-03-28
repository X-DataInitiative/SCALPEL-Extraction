package fr.polytechnique.cmap.cnam.study.pioglitazone

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{MedicalActs, MedicalActsConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{Diagnoses, DiagnosesConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.molecules.{MoleculePurchases, MoleculePurchasesConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.tracklosses.{Tracklosses, TracklossesConfig}
import fr.polytechnique.cmap.cnam.etl.filters.{EventFilters, PatientFilters}
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.MLPPLoader
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposureDefinition, ExposuresTransformer}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformer
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

    val conf = PioglitazoneConfig.load(argsMap("conf"), argsMap("env"))
    val inputPaths = conf.input
    val outputPaths = conf.output
    logger.info("Input Paths: " + inputPaths.toString)
    logger.info("Output Paths: " + outputPaths.toString)
    logger.info("study config....")
    logger.info("===================================")

    logger.info("Reading sources")
    import implicits.SourceReader
    val sources: Sources = Sources.sanitize(sqlContext.readSources(inputPaths))

    logger.info("Extracting patients...")
    val patientsConfig = PatientsConfig(conf.base.ageReferenceDate)
    val patients: Dataset[Patient] = new Patients(patientsConfig).extract(sources).cache()

    logger.info("Extracting molecule events...")
    val moleculesConfig = MoleculePurchasesConfig(drugClasses = conf.drugs.drugCategories)
    val drugEvents: Dataset[Event[Molecule]] = new MoleculePurchases(moleculesConfig).extract(sources).cache()

    logger.info("Extracting diagnosis events...")
    val diagnosesConfig = DiagnosesConfig(conf.diagnoses.imbDiagnosisCodes,
      conf.diagnoses.codesMapDP,
      conf.diagnoses.codesMapDR,
      conf.diagnoses.codesMapDA)

    logger.info("Extracting medical acts...")
    val medicalActConfig = MedicalActsConfig(
      conf.medicalActs.dcirMedicalActCodes,
      conf.medicalActs.mcoCIM10MedicalActCodes,
      conf.medicalActs.mcoCCAMMedicalActCodes)
    val medicalActs = new MedicalActs(medicalActConfig).extract(sources)

    val diseaseEvents: Dataset[Event[Diagnosis]] = new Diagnoses(diagnosesConfig).extract(sources).cache()

    logger.info("Merging all events...")
    val allEvents: Dataset[Event[AnyEvent]] = unionDatasets(
      drugEvents.as[Event[AnyEvent]],
      diseaseEvents.as[Event[AnyEvent]]
    )

    logger.info("Extracting Tracklosses...")
    val tracklossConfig = TracklossesConfig(studyEnd = conf.base.studyEnd)
    val tracklosses = new Tracklosses(tracklossConfig).extract(sources).cache()

    logger.info("Writing patients...")
    patients.toDF.write.parquet(outputPaths.patients)

    logger.info("Writing events...")
    allEvents.toDF.write.parquet(outputPaths.flatEvents)

    logger.info("Extracting Observations...")
    val observations = new ObservationPeriodTransformer(conf.base.studyStart, conf.base.studyEnd)
      .transform(allEvents)
      .cache()

    logger.info("Extracting cancer outcomes...")
    val (outcomeName, outcomes) = conf.outcomes.cancerDefinition match {
      case "broad" => (BroadBladderCancer.outcomeName, BroadBladderCancer.transform(diseaseEvents))
      case "naive" => (NaiveBladderCancer.outcomeName, NaiveBladderCancer.transform(diseaseEvents))
      case "narrow" => (NarrowBladderCancer.outcomeName, NarrowBladderCancer.transform(diseaseEvents, medicalActs))
    }

    logger.info("Extracting Follow-up...")
    val patiensWithObservations = patients.joinWith(observations, patients.col("patientId") === observations.col("patientId"))

    val followups = new FollowUpTransformer(conf.exposures.startDelay, firstTargetDisease = true, Some("cancer"))
      .transform(patiensWithObservations, drugEvents, outcomes, tracklosses)
      .cache()

    logger.info("Filtering Patients...")
    val filteredPatients = {
      val firstFilterResult = if (conf.filters.filterDelayedEntries)
        patients.filterDelayedPatients(drugEvents, conf.base.studyStart, conf.filters.delayedEntryThreshold)
      else
        patients

      if (conf.filters.filterDiagnosedPatients)
        firstFilterResult.filterEarlyDiagnosedPatients(outcomes, followups, outcomeName)
      else
        patients
    }

    logger.info("Writing cancer outcomes...")
    outcomes.filterPatients(filteredPatients.idsSet)

    logger.info("Extracting Exposures...")
    val patientsWithFollowups = filteredPatients.joinWith(followups, followups.col("patientId") === patients.col("patientId"))

    val exposureDef = ExposureDefinition(
    studyStart = conf.base.studyStart,
    filterDelayedPatients = false,
    diseaseCode = "C67")
    val exposures = new ExposuresTransformer(exposureDef)
      .transform(patientsWithFollowups, drugEvents)
      .cache()

    logger.info("Writing Exposures...")
    exposures.write.parquet(outputPaths.exposures)

    logger.info("Extracting MLPP features...")
    val params = MLPPLoader.Params(minTimestamp = conf.base.studyStart, maxTimestamp = conf.base.studyEnd)
    MLPPLoader(params).load(outcomes, exposures, patients, outputPaths.mlppFeatures)

    Some(allEvents)
  }
}