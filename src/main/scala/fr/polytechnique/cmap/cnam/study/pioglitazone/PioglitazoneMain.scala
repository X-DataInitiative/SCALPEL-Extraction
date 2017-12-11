package fr.polytechnique.cmap.cnam.study.pioglitazone

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
import fr.polytechnique.cmap.cnam.study.StudyConfig
import fr.polytechnique.cmap.cnam.study.StudyConfig.{InputPaths, OutputPaths}
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.{Dataset, SQLContext}


object PioglitazoneMain extends Main {

  val appName: String = "Pioglitazone"

  /**
    * Arguments expected:
    *   "conf" -> "path/to/file.conf" (default: "$resources/filtering-default.conf")
    *   "env" -> "cnam" | "cmap" | "test" (default: "test")
    */
  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[Event[AnyEvent]]] = {

    import EventFilters._
    import PatientFilters._
    import sqlContext.implicits._

    // "get" returns an Option, then we can use foreach to gently ignore when the key was not found.
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))
    argsMap.get("study").foreach(sqlContext.setConf("study", _))

    val inputPaths: InputPaths = StudyConfig.inputPaths
    val outputPaths: OutputPaths = StudyConfig.outputPaths

    logger.info("Input Paths: " + inputPaths.toString)
    logger.info("Output Paths: " + outputPaths.toString)
    logger.info("study config....")
    val configPIO = PioglitazoneConfig.pioglitazoneParameters
    logger.info("===================================")

    logger.info("Reading sources")
    import implicits.SourceReader
    val sources: Sources = sqlContext.readSources(inputPaths)

    logger.info("Extracting patients...")
    val patientsConfig = PatientsConfig(configPIO.study.ageReferenceDate)
    val patients: Dataset[Patient] = new Patients(patientsConfig).extract(sources).cache()

    logger.info("Extracting molecule events...")
    val moleculesConfig = MoleculePurchasesConfig(drugClasses = configPIO.drugs.drugCategories)
    val drugEvents: Dataset[Event[Molecule]] = new MoleculePurchases(moleculesConfig).extract(sources).cache()

    logger.info("Extracting diagnosis events...")
    val diagnosesConfig = DiagnosesConfig(configPIO.diagnoses.imbDiagnosisCodes,
      configPIO.diagnoses.codesMapDP,
      configPIO.diagnoses.codesMapDR,
      configPIO.diagnoses.codesMapDA)

    logger.info("Extracting medical acts...")
    val medicalActConfig = MedicalActsConfig(
      configPIO.medicalActs.dcirMedicalActCodes,
      configPIO.medicalActs.mcoCIM10MedicalActCodes,
      configPIO.medicalActs.mcoCCAMMedicalActCodes)
    val medicalActs = new MedicalActs(medicalActConfig).extract(sources)

    val diseaseEvents: Dataset[Event[Diagnosis]] = new Diagnoses(diagnosesConfig).extract(sources).cache()

    logger.info("Merging all events...")
    val allEvents: Dataset[Event[AnyEvent]] = unionDatasets(
      drugEvents.as[Event[AnyEvent]],
      diseaseEvents.as[Event[AnyEvent]]
    )

    logger.info("Extracting Tracklosses...")
    val tracklossConfig = TracklossesConfig(studyEnd = configPIO.study.lastDate)
    val tracklosses = new Tracklosses(tracklossConfig).extract(sources).cache()

    logger.info("Writing patients...")
    patients.toDF.write.parquet(outputPaths.patients)

    logger.info("Writing events...")
    allEvents.toDF.write.parquet(outputPaths.flatEvents)

    logger.info("Extracting Observations...")
    val observations = new ObservationPeriodTransformer(configPIO.study.studyStart, configPIO.study.studyEnd)
      .transform(allEvents)
      .cache()

    logger.info("Extracting cancer outcomes...")
    val (outcomeName, outcomes) = configPIO.study.cancerDefinition match {
      case "broad" => (BroadBladderCancer.outcomeName, BroadBladderCancer.transform(diseaseEvents))
      case "naive" => (NaiveBladderCancer.outcomeName, NaiveBladderCancer.transform(diseaseEvents))
      case "narrow" => (NarrowBladderCancer.outcomeName, NarrowBladderCancer.transform(diseaseEvents, medicalActs))
    }

    logger.info("Extracting Follow-up...")
    val patiensWithObservations = patients.joinWith(observations, patients.col("patientId") === observations.col("patientId"))

    val followups = new FollowUpTransformer(configPIO.drugs.start_delay, firstTargetDisease = true, Some("cancer"))
      .transform(patiensWithObservations, drugEvents, outcomes, tracklosses)
      .cache()

    logger.info("Filtering Patients...")
    val filteredPatients = {
      val firstFilterResult = if (configPIO.filters.filter_delayed_entries)
        patients.filterDelayedPatients(drugEvents, configPIO.study.studyStart, configPIO.study.delayed_entry_threshold)
      else
        patients

      if (configPIO.filters.filter_diagnosed_patients)
        firstFilterResult.filterEarlyDiagnosedPatients(outcomes, followups, outcomeName)
      else
        patients
    }

    logger.info("Writing cancer outcomes...")
    outcomes.filterPatients(filteredPatients)

    logger.info("Extracting Exposures...")
    val patientsWithFollowups = filteredPatients.joinWith(followups, followups.col("patientId") === patients.col("patientId"))

    val exposureDef = ExposureDefinition(
    studyStart = configPIO.study.studyStart,
    filterDelayedPatients = false,
    diseaseCode = "C67")
    val exposures = new ExposuresTransformer(exposureDef)
      .transform(patientsWithFollowups, drugEvents)
      .cache()

    logger.info("Writing Exposures...")
    exposures.write.parquet(StudyConfig.outputPaths.exposures)

    logger.info("Extracting MLPP features...")
    val params = MLPPLoader.Params(minTimestamp = configPIO.study.studyStart, maxTimestamp = configPIO.study.studyEnd)
    MLPPLoader(params).load(outcomes, exposures, patients, StudyConfig.outputPaths.mlppFeatures)

    Some(allEvents)
  }
}