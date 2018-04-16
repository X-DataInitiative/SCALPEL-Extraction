package fr.polytechnique.cmap.cnam.study

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.etl.config.StudyConfig.{InputPaths, OutputPaths}

/**
  * @deprecated Replaced by ....etl.config.StudyConfig
  */
object StudyConfig {

  private lazy val conf: Config = {
    // This is a little hacky. In the future, it may be nice to find a better way.
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")
    val study: String = sqlContext.getConf("study", "pioglitazone")
    val defaultConfig =  ConfigFactory.parseResources("config/" + study + "/"+ study + ".conf").resolve().getConfig(environment)
    val newConfig = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()
    newConfig.withFallback(defaultConfig).resolve()
  }

  lazy val inputPaths = InputPaths(
    dcir = Some(conf.getString("paths.input.dcir")),
    mco = Some(conf.getString("paths.input.pmsi_mco")),
    had = Some(conf.getString("paths.input.pmsi_had")),
    ssr = Some(conf.getString("paths.input.pmsi_ssr")),
    irBen = Some(conf.getString("paths.input.ir_ben")),
    irImb = Some(conf.getString("paths.input.ir_imb")),
    irPha = Some(conf.getString("paths.input.ir_pha")),
    dosages = Some(conf.getString("paths.input.dosages"))
  )

  lazy val outputPaths = OutputPaths(
    root = conf.getString("paths.output.root"),
    patients = conf.getString("paths.output.patients"),
    flatEvents = conf.getString("paths.output.flat_events"),
    coxFeatures = conf.getString("paths.output.cox_features"),
    ltsccsFeatures = conf.getString("paths.output.ltsccs_features"),
    mlppFeatures = conf.getString("paths.output.mlpp_features"),
    outcomes = conf.getString("paths.output.outcomes"),
    exposures = conf.getString("paths.output.exposures")
  )
}
