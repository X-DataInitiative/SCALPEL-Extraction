package fr.polytechnique.cmap.cnam.study.rosiglitazone

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Diagnosis, Event, Molecule}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{Diagnoses, DiagnosesConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.molecules.{MoleculePurchases, MoleculePurchasesConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.tracklosses.{Tracklosses, TracklossesConfig}
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.MLPPLoader
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposureDefinition, ExposuresTransformer}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformer
import fr.polytechnique.cmap.cnam.study.StudyConfig
import fr.polytechnique.cmap.cnam.study.StudyConfig.{InputPaths, OutputPaths}
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.{Dataset, SQLContext}


object RosiglitazoneMain extends Main{

  val appName = "Rosiglitazone"

  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[Event[AnyEvent]]] = {
    import sqlContext.implicits._

    // "get" returns an Option, then we can use foreach to gently ignore when the key was not found.
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val inputPaths: InputPaths = StudyConfig.inputPaths
    val outputPaths: OutputPaths = StudyConfig.outputPaths

    logger.info("Input Paths: " + inputPaths.toString)
    logger.info("Output Paths: " + outputPaths.toString)
    logger.info("study config....")
    val configROSI = RosiglitazoneConfig.rosiglitazoneParameters
    logger.info("===================================")

    logger.info("Reading sources")
    import implicits.SourceReader
    val sources: Sources = sqlContext.readSources(inputPaths)

    logger.info("Extracting patients...")
    val patientsConfig = PatientsConfig(configROSI.study.ageReferenceDate)
    val patients: Dataset[Patient] = new Patients(patientsConfig).extract(sources).cache()

    logger.info("Extracting molecule events...")
    val moleculesConfig = MoleculePurchasesConfig(drugClasses = configROSI.drugs.drugCategories)
    val drugEvents: Dataset[Event[Molecule]] = new MoleculePurchases(moleculesConfig).extract(sources).cache()

    logger.info("Extracting diagnosis events...")
    val diagnosesConfig = DiagnosesConfig(
      List.empty[String],
      RosiglitazoneStudyCodes.primaryDiagCodeInfract,
      RosiglitazoneStudyCodes.primaryDiagCodeInfract,
      RosiglitazoneStudyCodes.primaryDiagCodeInfract
    )

    val diseaseEvents: Dataset[Event[Diagnosis]] = new Diagnoses(diagnosesConfig).extract(sources).cache()

    logger.info("Merging all events...")
    val allEvents: Dataset[Event[AnyEvent]] = unionDatasets(
      drugEvents.as[Event[AnyEvent]],
      diseaseEvents.as[Event[AnyEvent]]
    )

    logger.info("Extracting Tracklosses...")
    val tracklossConfig = TracklossesConfig(studyEnd = configROSI.study.lastDate)
    val tracklosses = new Tracklosses(tracklossConfig).extract(sources).cache()

    logger.info("Writing patients...")
    patients.toDF.write.parquet(outputPaths.patients)

    logger.info("Writing events...")
    allEvents.toDF.write.parquet(outputPaths.flatEvents)

    logger.info("Extracting heart problems outcomes...")
    val outcomes = configROSI.study.heartProblemDefinition match {
      case "infarctus" => Infarctus.transform(diseaseEvents)
    }

    logger.info("Writing heart problems outcomes...")
    outcomes.toDF.write.parquet(outputPaths.CancerOutcomes)

    logger.info("Extracting Observations...")
    val observations = new ObservationPeriodTransformer(configROSI.study.studyStart, configROSI.study.studyEnd)
      .transform(allEvents)
      .cache()

    logger.info("Extracting Follow-up...")
    val patientsWithObservations = patients.joinWith(observations, patients.col("patientId") === observations.col("patientId"))

    val followups = new FollowUpTransformer(configROSI.drugs.start_delay, firstTargetDisease =  true, Some("heart_problem"))
      .transform(patientsWithObservations, drugEvents, outcomes, tracklosses)
      .cache()

    logger.info("Extracting Exposures...")
    val patientsWithFollowups = patients.joinWith(followups, followups.col("patientId") === patients.col("patientId"))

    val exposureDef = ExposureDefinition(
      studyStart = configROSI.study.studyStart,
      filterDelayedPatients = false
    )

    val exposures = new ExposuresTransformer(exposureDef)
      .transform(patientsWithFollowups, drugEvents)
      .cache()

    logger.info("Writing Exposures...")
    exposures.write.parquet(StudyConfig.outputPaths.exposures)

    logger.info("Extracting MLPP features...")
    MLPPLoader().load(outcomes, exposures, patients, StudyConfig.outputPaths.mlppFeatures)

    Some(allEvents)

  }
}

