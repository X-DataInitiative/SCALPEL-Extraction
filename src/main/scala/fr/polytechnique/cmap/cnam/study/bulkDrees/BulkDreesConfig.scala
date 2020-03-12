// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulkDrees

import java.time.LocalDate

import pureconfig.generic.auto._
import fr.polytechnique.cmap.cnam.etl.config.{BaseConfig, ConfigLoader}
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.Cip13Level
import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.NgapActConfig
import fr.polytechnique.cmap.cnam.etl.extractors.prestations.PractitionerClaimSpecialityConfig

case class BulkDreesConfig(
  input: StudyConfig.InputPaths,
  output: StudyConfig.OutputPaths) extends StudyConfig {
  val base: BaseConfig = BulkDreesConfig.BaseConfig
  val drugs: DrugConfig = BulkDreesConfig.DrugsConfig
  val practionnerClaimSpeciality: PractitionerClaimSpecialityConfig = BulkDreesConfig.PractitionerClaimSpecialtiesConfig
  val ngapActConfig: NgapActConfig = BulkDreesConfig.NgapActsConfig
}

object BulkDreesConfig extends ConfigLoader {

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

  final object BaseConfig extends BaseConfig(
    ageReferenceDate = LocalDate.of(2008, 1, 1),
    studyStart = LocalDate.of(2008, 1, 1),
    studyEnd = LocalDate.of(2020, 1, 1)
  )

  final object DrugsConfig extends DrugConfig(level = Cip13Level, families= List.empty)

  final object PractitionerClaimSpecialtiesConfig extends PractitionerClaimSpecialityConfig(
    medicalSpeCodes= List.empty,
    nonMedicalSpeCodes = List.empty
  )

  final object NgapActsConfig extends NgapActConfig(actsCategories = List.empty)

}
