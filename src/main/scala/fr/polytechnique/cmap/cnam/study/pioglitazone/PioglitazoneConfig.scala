package fr.polytechnique.cmap.cnam.study.pioglitazone

import java.time.LocalDate
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.config.{BaseConfig, ConfigLoader, StudyConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActsConfig
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.molecules.MoleculePurchasesConfig
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposurePeriodStrategy, ExposuresTransformerConfig, WeightAggStrategy}
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformerConfig
import fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes.CancerDefinition

case class PioglitazoneConfig(
    input: StudyConfig.InputPaths,
    output: StudyConfig.OutputPaths,
    exposures: PioglitazoneConfig.ExposuresConfig = PioglitazoneConfig.ExposuresConfig(),
    outcomes: PioglitazoneConfig.OutcomesConfig = PioglitazoneConfig.OutcomesConfig(),
    filters: PioglitazoneConfig.FiltersConfig = PioglitazoneConfig.FiltersConfig())
  extends StudyConfig {

  // The following config items are not overridable by the config file
  val base: BaseConfig = PioglitazoneConfig.BaseConfig
  val molecules: MoleculePurchasesConfig = PioglitazoneConfig.MoleculePurchasesConfig
  val medicalActs: MedicalActsConfig = PioglitazoneConfig.MedicalActsConfig
  val diagnoses: DiagnosesConfig = PioglitazoneConfig.DiagnosesConfig
}

object PioglitazoneConfig extends ConfigLoader with PioglitazoneStudyCodes {

  /** Base fixed parameters for this study. */
  final object BaseConfig extends BaseConfig {
    val ageReferenceDate: LocalDate = LocalDate.of(2007, 1, 1)
    val studyStart: LocalDate = LocalDate.of(2006, 1, 1)
    val studyEnd: LocalDate = LocalDate.of(2010, 1, 1)
  }

  /** Fixed parameters needed for the Drugs extractors. */
  final object MoleculePurchasesConfig extends MoleculePurchasesConfig {
    val drugClasses: List[String] = List("A10")
    val maxBoxQuantity: Int = 10
  }

  /** Fixed parameters needed for the Diagnoses extractors. */
  final object DiagnosesConfig extends DiagnosesConfig {
    val dpCodes: List[String] = primaryDiagCode :: secondaryDiagCodes
    val drCodes: List[String] = primaryDiagCode :: secondaryDiagCodes
    val daCodes: List[String] = List(primaryDiagCode)
    val imbCodes: List[String] = List(primaryDiagCode)
  }

  /** Fixed parameters needed for the Medical Acts extractors. */
  final object MedicalActsConfig extends MedicalActsConfig {
    val dcirCodes: List[String] = dcirCCAMActCodes
    val mcoCIMCodes: List[String] = mcoCIM10ActCodes
    val mcoCCAMCodes: List[String] = mcoCCAMActCodes
    val mcoCECodes: List[String] = List()
  }

  /** Parameters needed for the Exposures transformer. */
  case class ExposuresConfig(
      minPurchases: Int = 2,
      startDelay: Period = 3.month,
      purchasesWindow: Period = 6.months) extends ExposuresTransformerConfig {

    val periodStrategy: ExposurePeriodStrategy = ExposurePeriodStrategy.Unlimited
    val endDelay: Option[Period] = Some(0.months)
    val weightAggStrategy: WeightAggStrategy = WeightAggStrategy.NonCumulative
    val endThreshold: Option[Period] = None
    val cumulativeExposureWindow: Option[Int] = None
    val cumulativeStartThreshold: Option[Int] = None
    val cumulativeEndThreshold: Option[Int] = None
    val dosageLevelIntervals: Option[List[Int]] = None
    val purchaseIntervals: Option[List[Int]] = None
  }

  /** Parameters needed for the Outcomes transformer. */
  case class OutcomesConfig(
      cancerDefinition: CancerDefinition = CancerDefinition.default) extends OutcomesTransformerConfig

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
