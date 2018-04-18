package fr.polytechnique.cmap.cnam.study.rosiglitazone

import org.apache.spark.sql.{Dataset, SQLContext}
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
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposuresTransformer, ExposuresTransformerConfig}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.{FollowUpTransformer, FollowUpTransformerConfig}
import fr.polytechnique.cmap.cnam.etl.transformers.observation.{ObservationPeriodTransformer, ObservationPeriodTransformerConfig}
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

object RosiglitazoneMain extends Main{

  val appName = "Rosiglitazone"

  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[Event[AnyEvent]]] = {
    import sqlContext.implicits._

    val conf = RosiglitazoneConfig.load(argsMap("conf"), argsMap("env"))
    val inputPaths = conf.input
    val outputPaths = conf.output
    logger.info("Input Paths: " + inputPaths.toString)
    logger.info("Output Paths: " + outputPaths.toString)
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
    val diagnosesConfig = DiagnosesConfig(conf.diagnoses.codesMapDP, conf.diagnoses.codesMapDR, conf.diagnoses.codesMapDA, conf.diagnoses.imbDiagnosisCodes)
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

    logger.info("Extracting heart problems outcomes...")
    val outcomes = conf.outcomes.heartProblemDefinition match {
      case "infarctus" => Infarctus.transform(diseaseEvents)
    }

    logger.info("Writing heart problems outcomes...")
    outcomes.toDF.write.parquet(outputPaths.outcomes)

    logger.info("Extracting Observations...")
    val observations = new ObservationPeriodTransformer(
      ObservationPeriodTransformerConfig(conf.base.studyStart, conf.base.studyEnd))
        .transform(allEvents)
        .cache()

    logger.info("Extracting Follow-up...")
    val patientsWithObservations = patients.joinWith(observations, patients.col("patientId") === observations.col("patientId"))

    val followups = new FollowUpTransformer(
      FollowUpTransformerConfig(conf.exposures.startDelay, firstTargetDisease =  true, Some("heart_problem")))
        .transform(patientsWithObservations, drugEvents, outcomes, tracklosses)
        .cache()

    logger.info("Extracting Exposures...")
    val patientsWithFollowups = patients.joinWith(followups, followups.col("patientId") === patients.col("patientId"))

    val exposureDef = ExposuresTransformerConfig()

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

