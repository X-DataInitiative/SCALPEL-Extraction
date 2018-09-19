package fr.polytechnique.cmap.cnam.study.fall.config

import java.time.LocalDate
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.config.BaseConfig
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActsConfig
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs._
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposurePeriodStrategy, ExposuresTransformerConfig, WeightAggStrategy}
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig.{DrugsConfig, ExposureConfig, FracturesConfig, SitesConfig}
import fr.polytechnique.cmap.cnam.study.fall.{BodySite, BodySites}

case class FallConfig(
    input: StudyConfig.InputPaths,
    output: StudyConfig.OutputPaths,
    drugs: DrugsConfig = FallConfig.DrugsConfig(),
    exposures: ExposureConfig = FallConfig.ExposureConfig(),
    sites: SitesConfig = FallConfig.SitesConfig(),
    patients: FallConfig.PatientsConfig = FallConfig.PatientsConfig(),
    outcomes: FracturesConfig = FallConfig.FracturesConfig()) extends StudyConfig{

  val base: BaseConfig = FallConfig.BaseConfig
  val medicalActs: MedicalActsConfig = FallConfig.MedicalActsConfig
  val diagnoses: DiagnosesConfig = DiagnosesConfig(sites.fracturesCodes, sites.fracturesCodes)
}

object FallConfig extends FallConfigLoader with FractureCodes {

  /** Base fixed parameters for this study. */
  final object BaseConfig extends BaseConfig(
    ageReferenceDate = LocalDate.of(2014, 1, 1),
    studyStart = LocalDate.of(2014, 1, 1),
    studyEnd = LocalDate.of(2017, 1, 1)
  )

  /** Fixed parameters needed for the Patients extractors. */
  case class PatientsConfig(
    ageReferenceDate: LocalDate = FallConfig.BaseConfig.ageReferenceDate,
    startGapInMonths: Int = 2,
    followupStartDelay: Int = 0)

  /** Fixed parameters needed for the acts extractors. */
  final object MedicalActsConfig extends MedicalActsConfig(
    dcirCodes = (NonHospitalizedFracturesCcam ++ CCAMExceptions).toList,
    mcoCECodes = (NonHospitalizedFracturesCcam ++ CCAMExceptions).toList,
    mcoCCAMCodes = List(),
    mcoCIMCodes = List()
  )

  /** parameters for outcomes transformer **/
  case class FracturesConfig(override val fallFrame: Period = 0.months) extends FracturesTransformerConfig(
    fallFrame = fallFrame
  )

  /** parameters needed for drugs extractor**/
  case class DrugsConfig(
    level: DrugClassificationLevel = TherapeuticLevel,
    families: List[DrugConfig] = List(Antihypertenseurs, Antidepresseurs, Neuroleptiques, Hypnotiques))

  /** Parameters needed for the Exposure Transformer**/
  case class ExposureConfig(
      override val minPurchases: Int = 1,
      override val startDelay: Period = 0.months,
      override val purchasesWindow: Period = 0.months,
      override val endThresholdGc: Option[Period] = Some(90.days),
      override val endThresholdNgc: Option[Period] = Some(30.days),
      override val endDelay: Option[Period] = Some(30.days)) extends ExposuresTransformerConfig (

    startDelay = startDelay,
    minPurchases = minPurchases,
    purchasesWindow = purchasesWindow,
    periodStrategy = ExposurePeriodStrategy.Limited,
    endThresholdGc = endThresholdGc,
    endThresholdNgc = endThresholdNgc,
    endDelay = endDelay,
    weightAggStrategy = WeightAggStrategy.NonCumulative,
    cumulativeExposureWindow = Some(0),
    cumulativeStartThreshold = Some(0),
    cumulativeEndThreshold = Some(0),
    dosageLevelIntervals = Some(List()),
    purchaseIntervals = Some(List())
  )

  /** Parameters needed for the diagnosesConfig **/
  case class SitesConfig(sites: List[BodySite] = List(BodySites)) {
    val fracturesCodes = BodySite.extractCIM10CodesFromSites(sites)
  }

  /**
    * Reads a configuration file and merges it with the default file.
    * @param path The path of the given file.
    * @param env The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of PioglitazoneConfig containing all parameters.
    */
  def load(path: String, env: String): FallConfig = {
    val defaultPath = "config/fall/default.conf"
    loadConfigWithDefaults[FallConfig](path, defaultPath, env)
  }
}
