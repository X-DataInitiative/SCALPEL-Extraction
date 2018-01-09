package fr.polytechnique.cmap.cnam.study.fall

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.tracklosses.{Tracklosses, TracklossesConfig}
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.{MLPPFeature, MLPPLoader}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposureDefinition, ExposuresTransformer}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformer
import fr.polytechnique.cmap.cnam.study.StudyConfig
import fr.polytechnique.cmap.cnam.study.pioglitazone.PioglitazoneConfig
import fr.polytechnique.cmap.cnam.study.pioglitazone.PioglitazoneMain._
import fr.polytechnique.cmap.cnam.util.functions._
import fr.polytechnique.cmap.cnam.etl.filters.{EventFilters, PatientFilters, PatientFiltersImplicits}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.patients._






object MLPPMain extends Main {


  val appName: String = "MLPPFeaturing"

  trait Env {
    val FeaturingPath: String
    val McoPath: String
    val McoCePath: String
    val DcirPath: String
    val IrImbPath: String
    val IrBenPath: String
    val RefDate: Timestamp
  }

  object CmapEnv extends Env {
    override val FeaturingPath = "/shared/Observapur/featuring/"
    val McoPath = "/shared/Observapur/staging/Flattening/flat_table/MCO"
    val McoCePath = "/shared/Observapur/staging/Flattening/flat_table/MCO_ACE"
    val DcirPath = "/shared/Observapur/staging/Flattening/flat_table/DCIR"
    val IrImbPath = "/shared/Observapur/staging/Flattening/single_table/IR_IMB_R"
    val IrBenPath = "/shared/Observapur/staging/Flattening/single_table/IR_BEN_R"
    val RefDate = makeTS(2010,1,1)
  }

  object FallEnv extends Env {
    override val FeaturingPath = "/shared/fall/featuring/"
    val McoPath = "/shared/fall/staging/flattening/flat_table/MCO"
    val McoCePath = "/shared/fall/staging/flattening/flat_table/MCO_ACE"
    val DcirPath = "/shared/fall/staging/flattening/flat_table/DCIR"
    val IrImbPath = "/shared/fall/staging/flattening/single_table/IR_IMB_R"
    val IrBenPath = "/shared/fall/staging/flattening/single_table/IR_BEN_R"
    val RefDate = makeTS(2015,1,1)
  }

  object TestEnv extends Env {
    override val FeaturingPath = "target/test/output/"
    val McoPath = "src/test/resources/test-input/MCO.parquet"
    val McoCePath = null
    val DcirPath = "src/test/resources/test-input/DCIR.parquet"
    val IrImbPath = "src/test/resources/test-input/IR_IMB_R.parquet"
    val IrBenPath = "src/test/resources/test-input/IR_BEN_R.parquet"
    val RefDate = makeTS(2006,1,1)
  }

  def getSource

  def getSource(sqlContext: SQLContext, env: Env): Sources = {
    Sources.read(
      sqlContext,
      irImbPath = env.IrImbPath,
      irBenPath = env.IrBenPath,
      dcirPath = env.DcirPath,
      pmsiMcoPath = env.McoPath,
      pmsiMcoCEPath = env.McoCePath
    )
  }

  def getEnv(argsMap: Map[String, String]): Env = {
    argsMap.getOrElse("env", "test") match {
      case "fall" => FallEnv
      case "cmap" => CmapEnv
      case "test" => TestEnv
    }
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[MLPPFeature]] = {

    import sqlContext.implicits._

    // "get" returns an Option, then we can use foreach to gently ignore when the key was not found.
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val env = getEnv(argsMap)
    val sources = getSource(sqlContext, env)

    val configPIO = PioglitazoneConfig.pioglitazoneParameters

    val outputPath: String = FallEnv.FeaturingPath

    val outcomes: Dataset[[Event[Outcome]] = sqlContext.read.parquet("").as[Event[Outcome]]
    val dcirFlat: DataFrame = sqlContext.read.parquet("")

    val patients: Dataset[Patient] = sqlContext.read.parquet("").as[Patient].cache()

    // todo: test if filter_lost_patients is true
    val tracklossEvents: Dataset[Trackloss] = ???
    val tracklossFlatEvents = ???

    val allEvents = ???

    val drugEvents = sqlContext.read.parquet("").as[Event[Molecule]]

    logger.info("Merging all events...")
    val allEvents: Dataset[Event[AnyEvent]] = unionDatasets(
      drugEvents.as[Event[AnyEvent]],
      outcomes.as[Event[AnyEvent]]
    )

    logger.info("Extracting Tracklosses...")
    val tracklossConfig = TracklossesConfig(studyEnd = configPIO.study.lastDate)
    val tracklosses = new Tracklosses(tracklossConfig).extract(sources).cache()


    logger.info("Extracting Observations...")
    val observations = new ObservationPeriodTransformer(configPIO.study.studyStart, configPIO.study.studyEnd)
      .transform(allEvents)
      .cache()

    logger.info("Extracting Follow-up...")
    val patiensWithObservations = patients.joinWith(observations, patients.col("patientId") === observations.col("patientId"))

    val followups = new FollowUpTransformer(configPIO.drugs.start_delay, firstTargetDisease = true, Some("cancer"))
      .transform(patiensWithObservations, drugEvents, outcomes, tracklosses)
      .cache()

    logger.info("Filtering Patients..."
    val filteredPatients = {
      val firstFilterResult = if (configPIO.filters.filter_delayed_entries)
        patients.fil(drugEvents, configPIO.study.studyStart, configPIO.study.delayed_entry_threshold)
      else
        patients

      if (configPIO.filters.filter_diagnosed_patients)
        firstFilterResult.filterEarlyDiagnosedPatients(outcomes, followups, outcomeName)
      else
        patients
    }


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
