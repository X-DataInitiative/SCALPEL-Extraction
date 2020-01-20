// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.config

import java.time.LocalDate
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import pureconfig.generic.auto._
import fr.polytechnique.cmap.cnam.etl.config.BaseConfig
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActsConfig
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification._
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.families.{Antidepresseurs, Antihypertenseurs, Hypnotiques, Neuroleptiques}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.{DrugClassificationLevel, TherapeuticLevel}
import fr.polytechnique.cmap.cnam.etl.transformers.exposures._
import fr.polytechnique.cmap.cnam.etl.transformers.interaction.InteractionTransformerConfig
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig.{DrugsConfig, ExposureConfig, FracturesConfig, InteractionConfig, SitesConfig}
import fr.polytechnique.cmap.cnam.study.fall.fractures.{BodySite, BodySites}

case class FallConfig(
  input: StudyConfig.InputPaths,
  output: StudyConfig.OutputPaths,
  base: BaseConfig,
  exposures: ExposureConfig = FallConfig.ExposureConfig(),
  drugs: DrugsConfig = FallConfig.DrugsConfig(),
  interactions: InteractionConfig = FallConfig.InteractionConfig(),
  sites: SitesConfig = FallConfig.SitesConfig(),
  patients: FallConfig.PatientsConfig = FallConfig.PatientsConfig(),
  outcomes: FracturesConfig = FallConfig.FracturesConfig(),
  runParameters: FallConfig.RunConfig = FallConfig.RunConfig()) extends StudyConfig {

  val medicalActs: MedicalActsConfig = FallConfig.MedicalActsConfig
  val diagnoses: DiagnosesConfig = DiagnosesConfig(
    sites.fracturesCodes,
    sites.fracturesCodes,
    daCodes = sites.fracturesCodes
  )
}

object FallConfig extends FallConfigLoader with FractureCodes {

  /**
    * Reads a configuration file and merges it with the default file.
    *
    * @param path The path of the given file.
    * @param env  The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of FallConfig containing all parameters.
    */
  def load(path: String, env: String): FallConfig = {
    val defaultPath = "config/fall/default.conf"
    loadConfigWithDefaults[FallConfig](path, defaultPath, env)
  }

  /** Fixed parameters needed for the Patients extractors. */
  case class PatientsConfig(
    ageReferenceDate: LocalDate = LocalDate.of(2015, 1, 1),
    startGapInMonths: Int = 2,
    followupStartDelay: Int = 0)

  /** parameters for outcomes transformer **/
  case class FracturesConfig(override val fallFrame: Period = 0.months) extends FracturesTransformerConfig(
    fallFrame = fallFrame
  )

  /** parameters needed for drugs extractor **/
  case class DrugsConfig(
    override val level: DrugClassificationLevel = TherapeuticLevel,
    override val families: List[DrugClassConfig] = List(
      Antihypertenseurs,
      Antidepresseurs,
      Neuroleptiques,
      Hypnotiques
    )) extends DrugConfig(level = level, families = families)

  /** Parameters needed for the Exposure Transformer **/
  case class ExposureConfig(
    override val exposurePeriodAdder: ExposurePeriodAdder = LimitedExposureAdder(
      startDelay = 0.days,
      15.days,
      90.days,
      30.days,
      PurchaseCountBased
    )
  ) extends ExposuresTransformerConfig(exposurePeriodAdder = exposurePeriodAdder)


  /** Parameters needed for the Interaction Transformer **/
  case class InteractionConfig(
    override val level: Int = 2
  ) extends InteractionTransformerConfig(level = level)

  /** Parameters needed for the diagnosesConfig **/
  case class SitesConfig(sites: List[BodySite] = List(BodySites)) {
    val fracturesCodes: List[String] = BodySite.extractCIM10CodesFromSites(sites)
  }

  /** Parameters if run the calculation of outcome or exposure **/
  case class RunConfig(
    outcome: List[String] = List("Acts", "Diagnoses", "Outcomes"),
    exposure: List[String] = List("Patients", "StartGapPatients", "DrugPurchases", "Exposures"),
    hospitalStay: List[String] = List("HospitalStay")) {
    //exposures
    val patients: Boolean = exposure contains "Patients"
    val drugPurchases: Boolean = exposure contains "DrugPurchases"
    val startGapPatients: Boolean = List("DrugPurchases", "Patients", "StartGapPatients").forall(exposure.contains)
    val exposures: Boolean = List("Patients", "DrugPurchases", "Exposures").forall(exposure.contains)
    //outcomes
    val diagnoses: Boolean = outcome contains "Diagnoses"
    val acts: Boolean = outcome contains "Acts"
    val outcomes: Boolean = List("Diagnoses", "Acts", "Outcomes").forall(outcome.contains)
    // Hospital Stays
    val hospitalStays: Boolean = hospitalStay contains "HospitalStay"
  }

  /** Fixed parameters needed for the acts extractors. */
  final object MedicalActsConfig extends MedicalActsConfig(
    dcirCodes = (NonHospitalizedFracturesCcam ++ CCAMExceptions).toList,
    mcoCECodes = (NonHospitalizedFracturesCcam ++ CCAMExceptions).toList,
    mcoCCAMCodes = CCAMExceptions.toList,
    mcoCIMCodes = List(),
    ssrCSARRCodes = List(),
    ssrCCAMCodes = List(),
    hadCCAMCodes = List(),
    ssrCECodes = List()
  )

}