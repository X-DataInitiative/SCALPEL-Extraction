package fr.polytechnique.cmap.cnam.study.bulk

import java.time.LocalDate

import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.config.{BaseConfig, ConfigLoader}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.Cip13Level
import fr.polytechnique.cmap.cnam.etl.extractors.prestations.PractitionerClaimSpecialityConfig

case class BulkConfig(
                       input: StudyConfig.InputPaths,
                       output: StudyConfig.OutputPaths) extends StudyConfig {
  val drugs: DrugConfig = BulkConfig.DrugsConfig
  val practionnerClaimSpeciality: PractitionerClaimSpecialityConfig = BulkConfig.PrestationsConfig
  val base: BaseConfig = BulkConfig.BaseConfig
}

object BulkConfig extends ConfigLoader {

  final object BaseConfig extends BaseConfig(
    ageReferenceDate = LocalDate.of(2014, 1, 1),
    studyStart = LocalDate.of(2014, 1, 1),
    studyEnd = LocalDate.of(2017, 1, 1)
  )

  final object DrugsConfig extends DrugConfig(
    level = Cip13Level,
    families = List.empty
  )

  final object PrestationsConfig extends PractitionerClaimSpecialityConfig(
    medicalSpeCodes = List.empty,
    nonMedicalSpeCodes = List.empty
  )

  def load(path: String, env: String): BulkConfig = {
    val defaultPath = "config/bulk/default.conf"
    loadConfigWithDefaults[BulkConfig](path, defaultPath, env)
  }

}
