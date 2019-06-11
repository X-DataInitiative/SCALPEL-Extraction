package fr.polytechnique.cmap.cnam.study.rosiglitazone

import pureconfig.ConfigReader
import com.typesafe.config.ConfigFactory
import pureconfig._
import java.time.LocalDate
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.config.{BaseConfig, ConfigLoader}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.molecules.MoleculePurchasesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.patients.PatientsConfig
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.observation._
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up._
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActsConfig
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugClassConfig, DrugClassificationLevel, DrugConfig, TherapeuticLevel}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposurePeriodStrategy, ExposuresTransformerConfig, WeightAggStrategy}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformerConfig
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformerConfig
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformerConfig
import fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes.OutcomeDefinition

case class RosiglitazoneConfig(
    input: StudyConfig.InputPaths,
    output: StudyConfig.OutputPaths,
    exposures: RosiglitazoneConfig.ExposuresConfig = RosiglitazoneConfig.ExposuresConfig(),
    outcomes: RosiglitazoneConfig.OutcomesConfig = RosiglitazoneConfig.OutcomesConfig(),
    filters: RosiglitazoneConfig.FiltersConfig = RosiglitazoneConfig.FiltersConfig())
  extends StudyConfig {

  // The following config items are not overridable by the config file
  val base: BaseConfig = RosiglitazoneConfig.BaseConfig
  val patients: PatientsConfig = RosiglitazoneConfig.PatientsConfig
  val molecules: MoleculePurchasesConfig = RosiglitazoneConfig.MoleculePurchasesConfig
  val diagnoses: DiagnosesConfig = RosiglitazoneConfig.DiagnosesConfig
  val observationPeriod: RosiglitazoneConfig.ObservationPeriodConfig = RosiglitazoneConfig.ObservationPeriodConfig()
  val followUp: RosiglitazoneConfig.FollowUpConfig = RosiglitazoneConfig.FollowUpConfig(outcomes.outcomeDefinition)
}

object RosiglitazoneConfig extends ConfigLoader with RosiglitazoneStudyCodes {

  // ENTERING HACKVILLE
  implicit val patientReader: ConfigReader[Option[Dataset[Tuple2[Patient, Event[FollowUp]]]]] = ConfigReader[String].map(
    site => None
  )
  implicit val noneReader: ConfigReader[Option[Dataset[Event[Drug]]]] = ConfigReader[String].map(
    site => None
  )
  // EXITING HACKVILLE

  /** Base fixed parameters for this study. */
  final object BaseConfig extends BaseConfig (
    ageReferenceDate = LocalDate.of(2010, 1, 1),
    studyStart = LocalDate.of(2009, 1, 1),
    studyEnd = LocalDate.of(2011, 1, 1)
  )

  /** Fixed parameters needed for the Drugs extractors. */
  final object MoleculePurchasesConfig extends MoleculePurchasesConfig(
    drugClasses = List("A10"),
    maxBoxQuantity = 10
  )

  /** Fixed parameters needed for the Patients extractors. */
  final object PatientsConfig extends PatientsConfig(
    ageReferenceDate = RosiglitazoneConfig.BaseConfig.ageReferenceDate
  )

  /** Fixed parameters needed for the Diagnoses extractors. */
  final object DiagnosesConfig extends DiagnosesConfig(
    dpCodes = infarctusDiagnosisCodes ++ diagCodeHeartFailure ++ diagCodeHeartComplication,
    drCodes = infarctusDiagnosisCodes ++ diagCodeHeartFailure ++ diagCodeHeartComplication,
    daCodes = infarctusDiagnosisCodes ++ diagCodeHeartFailure ++ diagCodeHeartComplication,
    imbCodes = List()
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
    outcomeDefinition: OutcomeDefinition,
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
    outcomeName = Some(outcomeDefinition.outcomeName)
  )

  /** Parameters needed for the Exposures transformer. */
  case class ExposuresConfig(
      override val patients: Option[Dataset[(Patient, Event[FollowUp])]] = None,
      override val dispensations: Option[Dataset[Event[Drug]]] = None,
      override val minPurchases: Int = 1,
      override val startDelay: Period = 0.month,
      override val purchasesWindow: Period = 0.months) extends ExposuresTransformerConfig[Drug](
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
      outcomeDefinition: OutcomeDefinition = OutcomeDefinition.default)
    extends OutcomesTransformerConfig

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
