package fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.config.mlpp.{MLPPLoaderConfig, MLPPLoaderConfigLoader}
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config.MLPPConfig.{BaseConfig, ExtraConfig}
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
  input: MLPPLoaderConfig.InputPaths,
  output: MLPPLoaderConfig.OutputPaths,
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
