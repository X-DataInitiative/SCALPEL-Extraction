package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config.MLPPConfig
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config.MLPPConfig.load
import fr.polytechnique.cmap.cnam.etl.patients.Patient

object MLPPMain extends Main {

  val appName: String = "MLPPFeaturing"

  class Input(sqlContext: SQLContext, conf: MLPPConfig) {

    import sqlContext.implicits._

    val patients: Dataset[Patient] = sqlContext.read.parquet(conf.input.patients.get).as[Patient]
    val outcomes: Dataset[Event[Outcome]] = sqlContext.read.parquet(conf.input.outcomes.get).as[Event[Outcome]]
    val exposures: Dataset[Event[Exposure]] = sqlContext.read.parquet(conf.input.exposures.get).as[Event[Exposure]]
  }

  def readInput(sqlContext: SQLContext, conf: MLPPConfig): Input = new Input(sqlContext, conf)

  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[Event[AnyEvent]]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val conf: MLPPConfig = load(argsMap.getOrElse("conf", ""), argsMap("env"))
    val inputData = readInput(sqlContext, conf)

    val outcomes: Dataset[Event[Outcome]] = inputData.outcomes

    val patients: Dataset[Patient] = inputData.patients

    val exposures: Dataset[Event[Exposure]] = inputData.exposures

    logger.info("Extracting MLPP features...")
    MLPPLoader(conf).load(outcomes, exposures, patients)
    None
  }
}
