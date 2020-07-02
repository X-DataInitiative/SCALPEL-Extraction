// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulkDrees

import java.time.LocalDate

import pureconfig.generic.auto._
import fr.polytechnique.cmap.cnam.etl.config.BaseConfig
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.DrugConfig
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.classification.DrugClassConfig
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.level.{Cip13Level, DrugClassificationLevel}

case class BulkDreesConfig(
  input: StudyConfig.InputPaths,
  output: StudyConfig.OutputPaths,
  drugs: DrugConfig= BulkDreesConfig.DrugsConfig()) extends StudyConfig {
  val base: BaseConfig = BulkDreesConfig.BaseConfig
}

object BulkDreesConfig extends BulkDreesConfigLoader {
  /**
    * Reads a configuration file and merges it with the default file.
    *
    * @param path The path of the given file.
    * @param env  The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of dreesChronicConfig containing all parameters.
    */
  def load(path: String, env: String): BulkDreesConfig = {
    val defaultPath = "config/bulkDrees/default.conf"
    loadConfigWithDefaults[BulkDreesConfig](path, defaultPath, env)
  }

  final case class DrugsConfig(
    override val level: DrugClassificationLevel = Cip13Level,
    override val families: List[DrugClassConfig] = List.empty
  ) extends DrugConfig(level = level, families = families)

  final object BaseConfig extends BaseConfig(
    ageReferenceDate = LocalDate.of(2008, 1, 1),
    studyStart = LocalDate.of(2008, 1, 1),
    studyEnd = LocalDate.of(2020, 1, 1)
  )
}
