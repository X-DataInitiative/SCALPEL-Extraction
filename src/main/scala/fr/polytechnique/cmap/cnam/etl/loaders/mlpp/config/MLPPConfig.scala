package fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.config.mlpp.{MLPPLoaderConfig, MLPPLoaderConfigLoader}
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config.MLPPConfig.{BaseConfig, ExtraConfig, InputPaths, OutputPaths}
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.functions.makeTS

/**
  * conf used in MLPP
  *
  * @param input  the path of input sources
  * @param output the root path of output
  * @param base   base params of MLPP
  * @param extra  extra params of MLPP
  */
case class MLPPConfig(
  input: InputPaths,
  output: OutputPaths,
  base: BaseConfig = BaseConfig(),
  extra: ExtraConfig = ExtraConfig()
) extends MLPPLoaderConfig


object MLPPConfig extends MLPPLoaderConfigLoader {

  /**
    * Base fixed parameters for MLPP
    */
  case class BaseConfig(
    bucketSize: Int = 1,
    lagCount: Int = 1,
    keepFirstOnly: Boolean = true,
    featuresAsList: Boolean = true)

  /**
    * these params depends on env
    */
  case class ExtraConfig(
    minTimestamp: Timestamp = makeTS(2006, 1, 1),
    maxTimestamp: Timestamp = makeTS(2009, 12, 31, 23, 59, 59),
    includeCensoredBucket: Boolean = false)

  case class InputPaths(
    patients: Option[String] = None,
    outcomes: Option[String] = None,
    exposures: Option[String] = None) extends MLPPLoaderConfig.InputPaths(patients, outcomes, exposures)

  case class OutputPaths(root: Path) extends MLPPLoaderConfig.OutputPaths(root) {
    //the paths as below are related to the root
    lazy val outcomes: String = s"$root/csv/Outcomes.csv"
    lazy val staticStaticOutcomes: String = s"$root/csv/StaticOutcomes.csv"
    lazy val outcomesLookup: String = s"$root/csv/OutcomesLookup.csv"
    lazy val staticExposuresParquet: String = s"$root/parquet/StaticExposures"
    lazy val staticExposuresCSV: String = s"$root/csv/StaticExposures.csv"
    lazy val sparseFeaturesParquet: String = s"$root/parquet/SparseFeatures"
    lazy val sparseFeaturesCSV: String = s"$root/csv/SparseFeatures.csv"
    lazy val moleculeLookup: String = s"$root/csv/MoleculeLookup.csv"
    lazy val patientsLookup: String = s"$root/csv/PatientsLookup.csv"
    lazy val metadata: String = s"$root/csv/metadata.csv"
    lazy val censoring: String = s"$root/csv/Censoring.csv"
  }

  /**
    * Reads a configuration file and merges it with the default file.
    *
    * @param path The path of the given file.
    * @param env  The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of PioglitazoneConfig containing all parameters.
    */
  def load(path: String, env: String): MLPPConfig = {
    val defaultPath = "config/mlpp/default.conf"
    loadConfigWithDefaults[MLPPConfig](path, defaultPath, env)
  }

}
