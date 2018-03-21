package fr.polytechnique.cmap.cnam.study.pioglitazone

import java.time.LocalDate
import fr.polytechnique.cmap.cnam.etl.config.{ConfigLoader, StudyConfig}

case class PioglitazoneConfig(
    inputPaths: StudyConfig.InputPaths,
    outputPaths: StudyConfig.OutputPaths,
    base: PioglitazoneConfig.BaseConfig = PioglitazoneConfig.BaseConfig(),
    drugs: PioglitazoneConfig.DrugsConfig = PioglitazoneConfig.DrugsConfig(),
    medicalActs: PioglitazoneConfig.MedicalActsConfig = PioglitazoneConfig.MedicalActsConfig(),
    outcomes: PioglitazoneConfig.OutcomesConfig = PioglitazoneConfig.OutcomesConfig(),
    diagnoses: PioglitazoneConfig.DiagnosesConfig = PioglitazoneConfig.DiagnosesConfig(),
    filters: PioglitazoneConfig.FiltersConfig = PioglitazoneConfig.FiltersConfig())
  extends StudyConfig

object PioglitazoneConfig extends ConfigLoader {

  case class BaseConfig(
    ageReferenceDate: LocalDate = LocalDate.of(2007, 1, 1),
    studyStart: LocalDate = LocalDate.of(2006, 1, 1),
    studyEnd: LocalDate = LocalDate.of(2010, 1, 1),
    lastDate: LocalDate = LocalDate.of(2009, 12, 31),
    delayed_entry_threshold: Int = 12 /* keep it, maybe remove the previous one and set false when this param is 0*/)

  case class DiagnosesConfig(
    codesMapDP: List[String] = List("C67", "C77", "C78", "C79"),
    codesMapDR: List[String] = List("C67", "C77", "C78", "C79"),
    codesMapDA: List[String] = List("C67"),
    imbDiagnosisCodes: List[String] = List("C67"))

  case class MedicalActsConfig(
    dcirMedicalActCodes: List[String] = List(),
    mcoCIM10MedicalActCodes: List[String] = List(),
    mcoCCAMMedicalActCodes: List[String] = List())

  case class OutcomesConfig(
    cancerDefinition: String = "naive")

  case class FiltersConfig(
    filterNeverSickPatients: Boolean = false,
    filterDiagnosedPatients: Boolean = true,
    diagnosedPatientsThreshold: Int = 6, // keep it, maybe remove the previous one and set false when this param is 0
    filterDelayedEntries: Boolean = true)

  case class DrugsConfig(
    minPurchases: Int = 1, // 1 or 2
    startDelay: Int = 0, // can vary from 0 to 3
    purchasesWindow: Int = 0, // always 0
    onlyFirst: Boolean = false /* can be always false and handled in python / C++, but not soon*/,
    drugCategories: List[String] = List("A10"))

  def load(path: String, env: String): PioglitazoneConfig = {
    val defaultPath = "config/pioglitazone/default.conf"
    loadConfigWithDefaults[PioglitazoneConfig](path, defaultPath, env)
  }
}
