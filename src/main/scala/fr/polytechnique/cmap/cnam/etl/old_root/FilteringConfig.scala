package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp
import scala.collection.JavaConverters._
import scala.util.Try
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.util.functions._
import fr.polytechnique.cmap.cnam.etl.exposures._
import fr.polytechnique.cmap.cnam.etl.transformer.{exposure => new_exposure}

object FilteringConfig {

/* Alternative option using vars instead of SQLContext:

  private var _conf: Config = _
  private var _path: String = ""
  private var _env: String = "test"
  final private val defaultConfig = ConfigFactory.parseResources("filtering-default.conf")

  def path = _path
  def env = _env

  def setPath(path: String): Unit = { _path = path }
  def setEnv(env: String): Unit = { _env = env }
  def init(path: String, env: String ): Unit = {
    _path = path
    _env = env
    init()
  }
  def init(): Unit = {
    _conf = {
      val defaultConfig = ConfigFactory.parseResources("filtering-default.conf")
      val config = ConfigFactory.parseFile(new java.io.File(path)).withFallback(defaultConfig).resolve()
      config.getConfig(env)
    }
  }

  def conf = _conf
*/

  private lazy val conf: Config = {
    // This is a little hacky. In the future, it may be nice to find a better way.
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")

    val defaultConfig = ConfigFactory.parseResources("config/filtering-default.conf").resolve().getConfig(environment)
    val newConfig = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()

    newConfig.withFallback(defaultConfig).resolve()
  }

  case class InputPaths(
    dcir: String,
    pmsiMco: String,
    pmsiHad: String,
    pmsiSsr: String,
    irBen: String,
    irImb: String,
    irPha: String,
    dosages: String
  )

  case class OutputPaths(
    root: String,
    patients: String,
    flatEvents: String,
    coxFeatures: String,
    ltsccsFeatures: String,
    mlppFeatures: String,
    NaiveBladderCancerOutcomes: String
  )

  case class Limits(
    minYear: Int,
    maxYear: Int,
    minMonth: Int,
    maxMonth: Int,
    minGender: Int,
    maxGender: Int,
    minAge: Int,
    maxAge: Int,
    maxQuantityIrpha:Int
  )

  case class Dates(
    ageReference: Timestamp,
    studyStart: Timestamp,
    studyEnd: Timestamp
  )

  case class TracklossDefinition(
    threshold: Int,
    delay: Int
  )

  lazy val reuseFlatEventsPath: Option[String] = Try(conf.getString("reuse_flat_events_path")).toOption
  lazy val drugCategories: List[String] = conf.getStringList("drug_categories").asScala.toList
  lazy val cancerDefinition: String  = conf.getString("cancer_definition")
  lazy val diseaseCode: String = conf.getString("disease_code")
  lazy val mcoDeathCode: Int = conf.getInt("mco_death_code")
  lazy val mainDiagnosisCodes: List[String] = conf.getStringList("main_diagnosis_codes").asScala.toList
  lazy val linkedDiagnosisCodes: List[String] = conf.getStringList("linked_diagnosis_codes").asScala.toList
  lazy val associatedDiagnosisCodes: List[String] = conf.getStringList("associated_diagnosis_codes").asScala.toList
  lazy val imbDiagnosisCodes: List[String] = conf.getStringList("imb_diagnosis_codes").asScala.toList

  lazy val inputPaths = InputPaths(
    dcir = conf.getString("paths.input.dcir"),
    pmsiMco = conf.getString("paths.input.pmsi_mco"),
    pmsiHad = conf.getString("paths.input.pmsi_had"),
    pmsiSsr = conf.getString("paths.input.pmsi_ssr"),
    irBen = conf.getString("paths.input.ir_ben"),
    irImb = conf.getString("paths.input.ir_imb"),
    irPha = conf.getString("paths.input.ir_pha"),
    dosages = conf.getString("paths.input.dosages")
  )
  lazy val outputPaths = OutputPaths(
    root = conf.getString("paths.output.root"),
    patients = conf.getString("paths.output.patients"),
    flatEvents = conf.getString("paths.output.flat_events"),
    coxFeatures = conf.getString("paths.output.cox_features"),
    ltsccsFeatures = conf.getString("paths.output.ltsccs_features"),
    mlppFeatures = conf.getString("paths.output.mlpp_features"),
    NaiveBladderCancerOutcomes = conf.getString("paths.output.naive_bladder_cancer_outcomes")
  )
  lazy val limits = Limits(
    minYear = conf.getInt("limits.min_year"),
    maxYear = conf.getInt("limits.max_year"),
    minMonth = conf.getInt("limits.min_month"),
    maxMonth = conf.getInt("limits.max_month"),
    minGender = conf.getInt("limits.min_gender"),
    maxGender = conf.getInt("limits.max_gender"),
    minAge = conf.getInt("limits.min_age"),
    maxAge = conf.getInt("limits.max_age"),
    maxQuantityIrpha = conf.getInt("limits.max_quantity_irpha")
  )
  lazy val dates = Dates(
    ageReference = makeTS(conf.getIntList("dates.age_reference").asScala.toList),
    studyStart = makeTS(conf.getIntList("dates.study_start").asScala.toList),
    studyEnd = makeTS(conf.getIntList("dates.study_end").asScala.toList)
  )
  lazy val tracklossDefinition = TracklossDefinition(
    threshold = conf.getInt("trackloss.threshold"),
    delay = conf.getInt("trackloss.delay")
  )

  def modelConfig(modelName: String): Config = conf.getConfig(modelName)


  lazy val exposuresConfig: ExposuresConfig = ExposuresConfig(
    studyStart = dates.studyStart,
    diseaseCode = diseaseCode,
    periodStrategy = ExposurePeriodStrategy.fromString(
      conf.getString("exposures.period_strategy")
    ),
    followUpDelay = conf.getInt("exposures.follow_up_delay"),
    minPurchases = conf.getInt("exposures.min_purchases"),
    purchasesWindow = conf.getInt("exposures.purchases_window"),
    startDelay = conf.getInt("exposures.start_delay"),
    weightAggStrategy = WeightAggStrategy.fromString(
      conf.getString("exposures.weight_strategy")
    ),
    filterDelayedPatients = conf.getBoolean("filters.delayed_entries"),
    cumulativeExposureWindow = conf.getInt("exposures.cumulative.window"),
    cumulativeStartThreshold = conf.getInt("exposures.cumulative.start_threshold"),
    cumulativeEndThreshold = conf.getInt("exposures.cumulative.end_threshold"),
    dosageLevelIntervals = conf.getIntList("exposures.cumulative.dosage_level_intervals").asScala.map(_.toInt).toList,
    purchaseIntervals = conf.getIntList("exposures.cumulative.purchase_intervals").asScala.map(_.toInt).toList
  )

}
