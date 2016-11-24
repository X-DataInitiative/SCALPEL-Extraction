package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.utilities.functions._

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
    mlppFeatures: String
  )

  case class Limits(
    minYear: Int,
    maxYear: Int,
    minMonth: Int,
    maxMonth: Int,
    minGender: Int,
    maxGender: Int,
    minAge: Int,
    maxAge: Int
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

  lazy val drugCategories: List[String] = conf.getStringList("drug_categories").asScala.toList
  lazy val cancerDefinition: String  = conf.getString("cancer_definition")
  lazy val diseaseCode: String = conf.getString("disease_code")
  lazy val mcoDeathCode: Int = conf.getInt("mco_death_code")
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
    mlppFeatures = conf.getString("paths.output.mlpp_features")
  )
  lazy val limits = Limits(
    minYear = conf.getInt("limits.min_year"),
    maxYear = conf.getInt("limits.max_year"),
    minMonth = conf.getInt("limits.min_month"),
    maxMonth = conf.getInt("limits.max_month"),
    minGender = conf.getInt("limits.min_gender"),
    maxGender = conf.getInt("limits.max_gender"),
    minAge = conf.getInt("limits.min_age"),
    maxAge = conf.getInt("limits.max_age")
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
}
