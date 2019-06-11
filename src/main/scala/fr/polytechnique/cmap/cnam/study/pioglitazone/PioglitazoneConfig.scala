package fr.polytechnique.cmap.cnam.study.pioglitazone

import pureconfig.ConfigReader
import com.typesafe.config.ConfigFactory
import java.time.LocalDate
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.config.{BaseConfig, ConfigLoader}
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActsConfig
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.molecules.MoleculePurchasesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.patients.PatientsConfig
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.observation._
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up._
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActsConfig
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level._
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposurePeriodStrategy, ExposuresTransformerConfig, WeightAggStrategy}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformerConfig
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformerConfig
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
  val patients: PatientsConfig = PioglitazoneConfig.PatientsConfig
  val molecules: MoleculePurchasesConfig = PioglitazoneConfig.MoleculePurchasesConfig
  val medicalActs: MedicalActsConfig = PioglitazoneConfig.MedicalActsConfig
  val diagnoses: DiagnosesConfig = PioglitazoneConfig.DiagnosesConfig
  val observationPeriod: PioglitazoneConfig.ObservationPeriodConfig = PioglitazoneConfig.ObservationPeriodConfig()
  val followUp : PioglitazoneConfig.FollowUpConfig = PioglitazoneConfig.FollowUpConfig()
}

object PioglitazoneConfig extends ConfigLoader with PioglitazoneStudyCodes {

  /**
    * Reads a configuration file and merges it with the default file.
    *
    * @param path The path of the given file.
    * @param env  The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of PioglitazoneConfig containing all parameters.
    */
  def load(path: String, env: String): PioglitazoneConfig = {
    val defaultPath = "config/pioglitazone/default.conf"
    loadConfigWithDefaults[PioglitazoneConfig](path, defaultPath, env)
  }

  // ENTERING HACKVILLE
  implicit val patientReader: ConfigReader[Option[Dataset[Tuple2[Patient, Event[FollowUp]]]]] = ConfigReader[String].map(
    site => None
  )
  implicit val noneReader: ConfigReader[Option[Dataset[Event[Molecule]]]] = ConfigReader[String].map(
    site => None
  )
  // EXITING HACKVILLE

  /** Base fixed parameters for this study. */
  final object BaseConfig extends BaseConfig(
    ageReferenceDate = LocalDate.of(2007, 1, 1),
    studyStart = LocalDate.of(2006, 1, 1),
    studyEnd = LocalDate.of(2009, 12, 31)
    //    ageReferenceDate = LocalDate.of(2012, 1, 1),
    //    studyStart = LocalDate.of(2011, 1, 1),
    //    studyEnd = LocalDate.of(2013  , 1, 1)
  )

  /** Fixed parameters needed for the Patients extractors. */
  final object PatientsConfig extends PatientsConfig(
    ageReferenceDate = PioglitazoneConfig.BaseConfig.ageReferenceDate,
    maxAge = 80,
    minAge = 40
  )

  /** Fixed parameters needed for the Drugs extractors. */
  final object MoleculePurchasesConfig extends MoleculePurchasesConfig(
    drugClasses = List("A10"),
    maxBoxQuantity = 10
  )

  /** Fixed parameters needed for the Diagnoses extractors. */
  final object DiagnosesConfig extends DiagnosesConfig(
    dpCodes = primaryDiagCode :: secondaryDiagCodes,
    drCodes = primaryDiagCode :: secondaryDiagCodes,
    daCodes = List(primaryDiagCode),
    imbCodes = List(primaryDiagCode)
  )

  /** Fixed parameters needed for the Medical Acts extractors. */
  final object MedicalActsConfig extends MedicalActsConfig(
    dcirCodes = dcirCCAMActCodes,
    mcoCIMCodes = mcoCIM10ActCodes,
    mcoCCAMCodes = mcoCCAMActCodes,
    mcoCECodes = List()
  )

  /** Fixed parameters needed for the ObservationPeriod transformer. */
  case class ObservationPeriodConfig(
    override val events: Option[Dataset[Event[AnyEvent]]] = None
  ) extends ObservationPeriodTransformerConfig(
    events = events,
    studyStart = BaseConfig.studyStart,
    studyEnd = BaseConfig.studyEnd
  )

  /** Fixed parameters needed for the FollowUp transformer. */
  case class FollowUpConfig(
    override val patients: Option[Dataset[(Patient, Event[ObservationPeriod])]] = None,
    override val dispensations: Option[Dataset[Event[Molecule]]] = None,
    override val outcomes: Option[Dataset[Event[Outcome]]] = None,
    override val tracklosses: Option[Dataset[Event[Trackloss]]] = None
      ) extends FollowUpTransformerConfig(
        patients = patients,
        dispensations = dispensations,
        outcomes = outcomes,
        tracklosses = tracklosses,
        delayMonths = 6,
        firstTargetDisease = true,
        outcomeName = Some("cancer")
  )

  /** Parameters needed for the Exposures transformer. */
  case class ExposuresConfig(
    override val patients: Option[Dataset[(Patient, Event[FollowUp])]] = None,
    override val dispensations: Option[Dataset[Event[Molecule]]] = None,
    override val minPurchases: Int = 2,
    override val startDelay: Period = 3.month,
    override val purchasesWindow: Period = 6.months) extends ExposuresTransformerConfig[Molecule](
    patients = patients,
    dispensations = dispensations,

    minPurchases = minPurchases,
    startDelay = startDelay,
    purchasesWindow = purchasesWindow,

    periodStrategy = ExposurePeriodStrategy.Unlimited,
    endThresholdGc = None,
    endThresholdNgc = None,
    endDelay = None,

    weightAggStrategy = WeightAggStrategy.NonCumulative,
    cumulativeExposureWindow = None,
    cumulativeStartThreshold = None,
    cumulativeEndThreshold = None,
    dosageLevelIntervals = None,
    purchaseIntervals = None
  )

  /** Parameters needed for the Outcomes transformer. */
  case class OutcomesConfig(
    cancerDefinition: CancerDefinition = CancerDefinition.default)
    extends OutcomesTransformerConfig

  /** Parameters needed for the Filters. */
  case class FiltersConfig(
    filterDiagnosedPatients: Boolean = true,
    filterDelayedEntries: Boolean = true,
    delayedEntryThreshold: Int = 12)

}
