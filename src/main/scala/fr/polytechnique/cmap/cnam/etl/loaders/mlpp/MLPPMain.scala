package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.{Dataset, SQLContext}

object MLPPMain extends Main {
  
  val appName: String = "MLPPFeaturing"

  trait Env {
    val featuringPath: String
    val patientsPath: String
    val outcomesPath: String
    val exposuresPath: String
    val studyStart: Timestamp
    val studyEnd: Timestamp
    val includeCensoredBucket: Boolean

  }

  object CmapEnv extends Env {
    override val featuringPath = "/shared/fall/featuring/"
    val patientsPath = "/shared/Observapur/featuring/patients-filtered"
    val outcomesPath = "/shared/Observapur/featuring/fractures/*"
    val exposuresPath = "/shared/Observapur/featuring/exposures"
    val studyStart = makeTS(2010, 1, 1)
    val studyEnd = makeTS(2011, 1, 1)
    val includeCensoredBucket = true

  }

  object FallEnv extends Env {
    override val featuringPath = "/shared/fall/staging/All/featuring/"
    val patientsPath = "/shared/fall/staging/2018-02-20/featuringPharamacoClasses/filter_patients/data"
    val outcomesPath = "/shared/fall/staging/2018-02-20/featuringPharamacoClasses/fractures/*_fractures/data"
    val exposuresPath = "/shared/fall/staging/2018-02-20/featuringPharamacoClasses/exposures/data"
    val studyStart = makeTS(2015, 1, 1)
    val studyEnd = makeTS(2016, 1, 1)
    val includeCensoredBucket = false
  }

  object TestEnv extends Env {
    override val featuringPath = "target/test/output/featuring/"
    val patientsPath = "src/test/resources/MLPP/patient"
    val outcomesPath = "src/test/resources/MLPP/outcome"
    val exposuresPath = "src/test/resources/MLPP/exposure"
    val studyStart = makeTS(2006, 1, 1)
    val studyEnd = makeTS(2006, 8, 1)
    val includeCensoredBucket = false
  }

  class Input(sqlContext: SQLContext, env: Env) {
    import sqlContext.implicits._
    val patients: Dataset[Patient] = sqlContext.read.parquet(env.patientsPath).as[Patient]
    val outcomes: Dataset[Event[Outcome]] = sqlContext.read.parquet(env.outcomesPath).as[Event[Outcome]]
    val exposures: Dataset[Event[Exposure]] = sqlContext.read.parquet(env.exposuresPath).as[Event[Exposure]]
  }

  def getEnv(argsMap: Map[String, String]): Env = {
    argsMap.getOrElse("env", "test") match {
      case "fall" => FallEnv
      case "cmap" => CmapEnv
      case "test" => TestEnv
    }
}

  def readInput(sqlContext: SQLContext, env: Env): Input = new Input(sqlContext, env)

  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[Event[AnyEvent]]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val env = getEnv(argsMap)
    val inputData = readInput(sqlContext, env)

    val outcomes: Dataset[Event[Outcome]] = inputData.outcomes

    val patients: Dataset[Patient] = inputData.patients

    val exposures: Dataset[Event[Exposure]] = inputData.exposures

    logger.info("Extracting MLPP features...")
    val params = MLPPLoader.Params(
      includeCensoredBucket = env.includeCensoredBucket,
      minTimestamp = env.studyStart,
      maxTimestamp = env.studyEnd,
      featuresAsList = true)
    MLPPLoader(params).load(outcomes, exposures, patients, env.featuringPath)
    None
  }
}
