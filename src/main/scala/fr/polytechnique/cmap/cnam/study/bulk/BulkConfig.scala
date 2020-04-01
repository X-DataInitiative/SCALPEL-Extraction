// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk

import java.time.LocalDate
import pureconfig.generic.auto._
import fr.polytechnique.cmap.cnam.etl.config.BaseConfig
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.DrugConfig
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.classification.DrugClassConfig
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.level.{Cip13Level, DrugClassificationLevel}

case class BulkConfig(
  input: StudyConfig.InputPaths,
  output: StudyConfig.OutputPaths,
  drugs: DrugConfig = BulkConfig.DrugsConfig()) extends StudyConfig {
  val base: BaseConfig = BulkConfig.BaseConfig
}

object BulkConfig extends BulkConfigLoader {

  def load(path: String, env: String): BulkConfig = {
    val defaultPath = "config/bulk/default.conf"
    loadConfigWithDefaults[BulkConfig](path, defaultPath, env)
  }

  final case class DrugsConfig(
    override val level: DrugClassificationLevel = Cip13Level,
    override val families: List[DrugClassConfig] = List.empty
  ) extends DrugConfig(level = level, families = families)

  final object BaseConfig extends BaseConfig(
    ageReferenceDate = LocalDate.of(2011, 1, 1),
    studyStart = LocalDate.of(2010, 1, 1),
    studyEnd = LocalDate.of(2015, 1, 1)
  )

}
