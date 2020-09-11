// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.rosiglitazone

import java.time.LocalDate
import me.danielpes.spark.datetime.implicits._
import pureconfig.generic.auto._
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig
import fr.polytechnique.cmap.cnam.etl.config.{BaseConfig, ConfigLoader}
import fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.events.molecules.MoleculePurchasesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.patients.PatientsConfig
import fr.polytechnique.cmap.cnam.etl.transformers.exposures._
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformerConfig
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformerConfig
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformerConfig
import fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes.OutcomeDefinition

case class RosiglitazoneConfig(
  input: StudyConfig.InputPaths,
  output: StudyConfig.OutputPaths,
  exposures: RosiglitazoneConfig.ExposureConfig = RosiglitazoneConfig.ExposureConfig(),
  outcomes: RosiglitazoneConfig.OutcomesConfig = RosiglitazoneConfig.OutcomesConfig(),
  filters: RosiglitazoneConfig.FiltersConfig = RosiglitazoneConfig.FiltersConfig(),
  readFileFormat: String = "parquet",
  writeFileFormat: String = "parquet")
  extends StudyConfig {

  // The following config items are not overridable by the config file
  val base: BaseConfig = RosiglitazoneConfig.BaseConfig
  val patients: PatientsConfig = RosiglitazoneConfig.PatientsConfig
  val molecules: MoleculePurchasesConfig = RosiglitazoneConfig.MoleculePurchasesConfig
  val diagnoses: DiagnosesConfig = RosiglitazoneConfig.DiagnosesConfig
  val observationPeriod: ObservationPeriodTransformerConfig = RosiglitazoneConfig.ObservationPeriodTransformerConfig
  val followUp: FollowUpTransformerConfig = RosiglitazoneConfig.FollowUpConfig(outcomes.outcomeDefinition)
}

object RosiglitazoneConfig extends ConfigLoader with RosiglitazoneStudyCodes {

  /**
    * Reads a configuration file and merges it with the default file.
    *
    * @param path The path of the given file.
    * @param env  The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of RosiglitazoneConfig containing all parameters.
    */
  def load(path: String, env: String): RosiglitazoneConfig = {
    val defaultPath = "config/rosiglitazone/default.conf"
    loadConfigWithDefaults[RosiglitazoneConfig](path, defaultPath, env)
  }

  /** Fixed parameters needed for the FollowUp transformer. */
  case class FollowUpConfig(
    outcomeDefinition: OutcomeDefinition) extends FollowUpTransformerConfig(
    delayMonths = 6,
    firstTargetDisease = true,
    outcomeName = Some(outcomeDefinition.outcomeName)
  )

  /** Parameters needed for the Exposures transformer. */
  case class ExposureConfig(
    override val exposurePeriodAdder: ExposurePeriodAdder = UnlimitedExposureAdder(
      startDelay = 3.months,
      2,
      6.months
    )
  ) extends ExposuresTransformerConfig(exposurePeriodAdder = exposurePeriodAdder)

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

  /** Base fixed parameters for this study. */
  final object BaseConfig extends BaseConfig(
    ageReferenceDate = LocalDate.of(2011, 1, 1),
    studyStart = LocalDate.of(2010, 1, 1),
    studyEnd = LocalDate.of(2015, 1, 1)
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
  final object ObservationPeriodTransformerConfig extends ObservationPeriodTransformerConfig(
    studyStart = BaseConfig.studyStart,
    studyEnd = BaseConfig.studyEnd
  )

}
