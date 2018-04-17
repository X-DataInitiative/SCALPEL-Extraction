package fr.polytechnique.cmap.cnam.study.rosiglitazone

import java.time.LocalDate
import fr.polytechnique.cmap.cnam.etl.config.{ConfigLoader, StudyConfig}

case class RosiglitazoneConfig(
    input: StudyConfig.InputPaths,
    output: StudyConfig.OutputPaths,
    exposures: RosiglitazoneConfig.ExposuresConfig = RosiglitazoneConfig.ExposuresConfig(),
    outcomes: RosiglitazoneConfig.OutcomesConfig = RosiglitazoneConfig.OutcomesConfig(),
    filters: RosiglitazoneConfig.FiltersConfig = RosiglitazoneConfig.FiltersConfig())
  extends StudyConfig {

  // The following config items are not overridable by the config file
  val base = RosiglitazoneConfig.BaseConfig
  val drugs = RosiglitazoneConfig.DrugsConfig
  val diagnoses = RosiglitazoneConfig.DiagnosesConfig
}

object RosiglitazoneConfig extends ConfigLoader {

  /** Base fixed parameters for this study. */
  final object BaseConfig {
    val ageReferenceDate: LocalDate = LocalDate.of(2010, 1, 1)
    val studyStart: LocalDate = LocalDate.of(2009, 1, 1)
    val studyEnd: LocalDate = LocalDate.of(2011, 1, 1)
  }

  /** Fixed parameters needed for the Drugs extractors. */
  final object DrugsConfig {
    val drugCategories: List[String] = List("A10")
  }

  /** Fixed parameters needed for the Diagnoses extractors. */
  final object DiagnosesConfig {
    val codesMapDP: List[String] = RosiglitazoneStudyCodes.diagCodeInfarct
    val codesMapDR: List[String] = RosiglitazoneStudyCodes.diagCodeInfarct
    val codesMapDA: List[String] = RosiglitazoneStudyCodes.diagCodeInfarct
    val imbDiagnosisCodes: List[String] = List()
  }
  /** Parameters needed for the Exposures transformer. */
  case class ExposuresConfig(
    minPurchases: Int = 1,
    startDelay: Int = 0,
    purchasesWindow: Int = 0,
    onlyFirst: Boolean = false)

  /** Parameters needed for the Outcomes transformer. */
  case class OutcomesConfig(
    heartProblemDefinition: String = "infarctus")

  /** Parameters needed for the Filters. */
  case class FiltersConfig(
    filterNeverSickPatients: Boolean = false,
    filterDiagnosedPatients: Boolean = true,
    diagnosedPatientsThreshold: Int = 6,
    filterDelayedEntries: Boolean = true,
    delayedEntryThreshold: Int = 12)

  /**
    * Reads a configuration file and merges it with the default file.
    * @param path The path of the given file.
    * @param env The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of RosiglitazoneConfig containing all parameters.
    */
  def load(path: String, env: String): RosiglitazoneConfig = {
    val defaultPath = "config/rosiglitazone/default.conf"
    loadConfigWithDefaults[RosiglitazoneConfig](path, defaultPath, env)
  }
}
