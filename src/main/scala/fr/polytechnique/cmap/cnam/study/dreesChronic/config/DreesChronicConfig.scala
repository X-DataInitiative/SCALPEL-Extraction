package fr.polytechnique.cmap.cnam.study.dreesChronic.config

import java.time.LocalDate



//import me.danielpes.spark.datetime.Period
//import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.config.BaseConfig
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActsConfig
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.prestations.PractitionerClaimSpecialityConfig
import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.NgapActConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification._
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.{DrugClassificationLevel, PharmacologicalLevel}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugConfig
import fr.polytechnique.cmap.cnam.study.dreesChronic.codes._
import fr.polytechnique.cmap.cnam.study.dreesChronic.config.DreesChronicConfig.{BaseConfig, DrugsConfig, PatientsConfig, RunConfig}

case class DreesChronicConfig(
   input: StudyConfig.InputPaths,
   output: StudyConfig.OutputPaths,
   drugs: DrugsConfig = DreesChronicConfig.DrugsConfig(),
   patients: PatientsConfig = PatientsConfig(),
   runParameters: RunConfig = RunConfig()) extends StudyConfig {

  val base: BaseConfig = BaseConfig
  val medicalActs: MedicalActsConfig = DreesChronicConfig.MedicalActsConfig
  val diagnoses: DiagnosesConfig = DreesChronicConfig.DiagnosesConfig
  val practionnerClaimSpeciality: PractitionerClaimSpecialityConfig = DreesChronicConfig.PrestationsConfig
  val ngapActs: NgapActConfig = DreesChronicConfig.NgapActConfig
}

object DreesChronicConfig extends DreesChronicConfigLoader with BpcoCodes {

  /** Base fixed parameters for this study. */
  final object BaseConfig extends BaseConfig(
    // patient age should be taken at year N-1 thus in 2015 if study on 2014-2016
    ageReferenceDate = LocalDate.of(2008, 1, 1),
    studyStart = LocalDate.of(2008, 1, 1),
    studyEnd = LocalDate.of(2017, 1, 1)
  )

  /** Fixed parameters needed for the Patients extractors. */
  case class PatientsConfig(
    ageReferenceDate: LocalDate = DreesChronicConfig.BaseConfig.ageReferenceDate,
    minAge: Int = 40,
    startGapInMonths: Int = 0,
    followupStartDelay: Int = 0
   )

  /** Fixed parameters needed for the Diagnoses extractors. */
  final object DiagnosesConfig extends DiagnosesConfig(
    dpCodes = primaryDiagCodes ::: secondaryDiagCodes ::: otherCIM10Codes  ,
    drCodes = primaryDiagCodes ::: secondaryDiagCodes ::: otherCIM10Codes,
    daCodes = primaryDiagCodes ::: secondaryDiagCodes ::: otherCIM10Codes,
    imbCodes = ALDcodes
  )

  /** Fixed parameters needed for the Prestation extractors. */
  final object PrestationsConfig extends PractitionerClaimSpecialityConfig(
    medicalSpeCodes = speCodes,
    nonMedicalSpeCodes = nonSpeCodes
  )

  final object NgapActConfig extends NgapActConfig(
    acts_categories = List(
      Amk,
      Ams
    )
  )

  /** Fixed parameters needed for the acts extractors. */
  final object MedicalActsConfig extends MedicalActsConfig(
    dcirCodes = otherCCAMCodes ++ efrCCAMCodes ++ gazSangCCAMCodes,
    mcoCECodes = otherCCAMCodes ++ efrCCAMCodes ++ gazSangCCAMCodes,
    mcoCCAMCodes = otherCCAMCodes ++ efrCCAMCodes ++ gazSangCCAMCodes,
    mcoCIMCodes = List(),
    ssrCSARRCodes = List(),
    ssrCCAMCodes = otherCCAMCodes ++ efrCCAMCodes ++ gazSangCCAMCodes,
    hadCCAMCodes = otherCCAMCodes ++ efrCCAMCodes ++ gazSangCCAMCodes,
    ssrCECodes = otherCCAMCodes ++ efrCCAMCodes ++ gazSangCCAMCodes
  )

  /** parameters needed for drugs extractor **/
  case class DrugsConfig(
    override val level: DrugClassificationLevel = PharmacologicalLevel,
    override val families: List[DrugClassConfig] = List(
      Antibiotiques,
      Bronchodilatateurs,
      Nicotiniques,
      Traitements,
      Corticoides
    )) extends DrugConfig(level = level, families = families)


  /** Parameters if run the calculation of outcome or exposure **/
  case class RunConfig(
    outcome: List[String] = List("Acts", "Diagnoses", "Outcomes", "GhmGroups"),
    exposure: List[String] = List("Patients", "StartGapPatients", "DrugPurchases", "Exposures"),
    hospitalStay: List[String] = List("HospitalStay"),
    prestation: List[String] =  List("Specialities", "NgapAct")) {

    //exposures
    val patients: Boolean = exposure contains "Patients"
    val drugPurchases: Boolean = exposure contains "DrugPurchases"
    val hospitalStays: Boolean = hospitalStay contains "HospitalStay"
    val startGapPatients: Boolean = List("DrugPurchases", "Patients", "StartGapPatients").forall(exposure.contains)
    //outcomes
    val diagnoses: Boolean = outcome contains "Diagnoses"
    val acts: Boolean = outcome contains "Acts"
    // prestations
    val practionnerClaimSpeciality: Boolean = prestation contains "Specialities"
    val ngapActs: Boolean = prestation contains "NgapAct"
    val outcomes: Boolean = List("Diagnoses", "Acts", "Outcomes").forall(outcome.contains)
    val ghmGroups: Boolean = outcome contains "GhmGroups"
  }

  /**
    * Reads a configuration file and merges it with the default file.
    *
    * @param path The path of the given file.
    * @param env  The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of dreesChronicConfig containing all parameters.
    */
  def load(path: String, env: String): DreesChronicConfig = {
    val defaultPath = "config/dreesChronic/default.conf"
    loadConfigWithDefaults[DreesChronicConfig](path, defaultPath, env)
  }
}