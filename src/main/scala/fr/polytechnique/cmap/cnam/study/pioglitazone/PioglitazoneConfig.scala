package fr.polytechnique.cmap.cnam.study.pioglitazone

import java.time.LocalDate
import fr.polytechnique.cmap.cnam.etl.config.{ConfigLoader, StudyConfig}

case class PioglitazoneConfig(
    input: StudyConfig.InputPaths,
    output: StudyConfig.OutputPaths,
    exposures: PioglitazoneConfig.ExposuresConfig = PioglitazoneConfig.ExposuresConfig(),
    outcomes: PioglitazoneConfig.OutcomesConfig = PioglitazoneConfig.OutcomesConfig(),
    filters: PioglitazoneConfig.FiltersConfig = PioglitazoneConfig.FiltersConfig())
  extends StudyConfig {

  // The following config items are not overridable by the config file
  val base = PioglitazoneConfig.BaseConfig
  val drugs = PioglitazoneConfig.DrugsConfig
  val medicalActs = PioglitazoneConfig.MedicalActsConfig
  val diagnoses = PioglitazoneConfig.DiagnosesConfig
}

object PioglitazoneConfig extends ConfigLoader with PioglitazoneStudyCodes {

  /** Base fixed parameters for this study. */
  final object BaseConfig {
    val ageReferenceDate: LocalDate = LocalDate.of(2007, 1, 1)
    val studyStart: LocalDate = LocalDate.of(2006, 1, 1)
    val studyEnd: LocalDate = LocalDate.of(2010, 1, 1)
  }

  /** Fixed parameters needed for the Drugs extractors. */
  final object DrugsConfig {
    val drugCategories: List[String] = List("A10")
  }

  /** Fixed parameters needed for the Diagnoses extractors. */
  final object DiagnosesConfig {
    val codesMapDP: List[String] = primaryDiagCode :: secondaryDiagCodes
    val codesMapDR: List[String] = primaryDiagCode :: secondaryDiagCodes
    val codesMapDA: List[String] = List(primaryDiagCode)
    val imbDiagnosisCodes: List[String] = List(primaryDiagCode)
  }

  /** Fixed parameters needed for the Medical Acts extractors. */
  final object MedicalActsConfig {
    val dcirMedicalActCodes: List[String] = dcirCCAMActCodes
    val mcoCIM10MedicalActCodes: List[String] = mcoCIM10ActCodes
    val mcoCCAMMedicalActCodes: List[String] = mcoCCAMActCodes
  }

  /** Parameters needed for the Exposures transformer. */
  case class ExposuresConfig(
    minPurchases: Int = 1,
    startDelay: Int = 0,
    purchasesWindow: Int = 0,
    onlyFirst: Boolean = false)

  /** Parameters needed for the Outcomes transformer. */
  case class OutcomesConfig(
    cancerDefinition: String = "naive")

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
    * @return An instance of PioglitazoneConfig containing all parameters.
    */
  def load(path: String, env: String): PioglitazoneConfig = {
    val defaultPath = "config/pioglitazone/default.conf"
    loadConfigWithDefaults[PioglitazoneConfig](path, defaultPath, env)
  }
}
