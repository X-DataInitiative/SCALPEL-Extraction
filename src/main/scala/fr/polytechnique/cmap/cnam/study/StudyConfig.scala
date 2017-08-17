package fr.polytechnique.cmap.cnam.study

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object StudyConfig {
  private lazy val conf: Config = {
    // This is a little hacky. In the future, it may be nice to find a better way.
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")
    val study: String = sqlContext.getConf("study", "pioglitazone")
    ConfigFactory.parseResources("config/" + study + "/"+ study + ".conf").resolve().getConfig(environment)
  }

  case class InputPaths(
                         dcir: String,
                         pmsiMco: String,
                         pmsiHad: String,
                         pmsiSsr: String,
                         irBen: String,
                         irImb: String,
                         irPha: String,
                         dosages: String
                       )

  case class OutputPaths(
                          root: String,
                          patients: String,
                          flatEvents: String,
                          coxFeatures: String,
                          ltsccsFeatures: String,
                          mlppFeatures: String,
                          CancerOutcomes: String,
                          exposures: String
                        )
  lazy val inputPaths = InputPaths(
    dcir = conf.getString("paths.input.dcir"),
    pmsiMco = conf.getString("paths.input.pmsi_mco"),
    pmsiHad = conf.getString("paths.input.pmsi_had"),
    pmsiSsr = conf.getString("paths.input.pmsi_ssr"),
    irBen = conf.getString("paths.input.ir_ben"),
    irImb = conf.getString("paths.input.ir_imb"),
    irPha = conf.getString("paths.input.ir_pha"),
    dosages = conf.getString("paths.input.dosages")
  )

  lazy val outputPaths = OutputPaths(
    root = conf.getString("paths.output.root"),
    patients = conf.getString("paths.output.patients"),
    flatEvents = conf.getString("paths.output.flat_events"),
    coxFeatures = conf.getString("paths.output.cox_features"),
    ltsccsFeatures = conf.getString("paths.output.ltsccs_features"),
    mlppFeatures = conf.getString("paths.output.mlpp_features"),
    CancerOutcomes = conf.getString("paths.output.cancer_outcomes"),
    exposures = conf.getString("paths.output.exposures")
  )
}
