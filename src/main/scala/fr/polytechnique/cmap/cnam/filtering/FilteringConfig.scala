package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.utilities.functions._

object FilteringConfig {

  final private val Conf: Config = {
    // This is a little hacky. In the future, we need to find a better way.
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("config_path")
    val environment: String = sqlContext.getConf("environment")
    val config = ConfigFactory.parseFile(new java.io.File(configPath))
    config.getConfig(environment)
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

  case class OutputPaths(root: String, patients: String, flatEvents: String)

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

  case class Dates(ageReference: Timestamp)

  val drugCategories: List[String] = Conf.getStringList("shared.drug_categories").asScala.toList
  val cancerDefinition: String  = Conf.getString("shared.cancerDefinition")
  val diseaseCode: String = Conf.getString("shared.diseaseCode")
  val mcoDeathCode: Int = Conf.getInt("shared.mco_death_code")
  val inputPaths = InputPaths(
    dcir = Conf.getString("paths.input.dcir"),
    pmsiMco = Conf.getString("paths.input.pmsi_mco"),
    pmsiHad = Conf.getString("paths.input.pmsi_had"),
    pmsiSsr = Conf.getString("paths.input.pmsi_ssr"),
    irBen = Conf.getString("paths.input.ir_ben"),
    irImb = Conf.getString("paths.input.ir_imb"),
    irPha = Conf.getString("paths.input.ir_pha"),
    dosages = Conf.getString("paths.input.dosages")
  )
  val outputPaths = OutputPaths(
    root = Conf.getString("paths.output.root"),
    patients = Conf.getString("paths.output.patients"),
    flatEvents = Conf.getString("paths.output.flat_events")
  )
  val limits = Limits(
    minYear = Conf.getInt("shared.limits.min_year"),
    maxYear = Conf.getInt("shared.limits.max_year"),
    minMonth = Conf.getInt("shared.limits.min_month"),
    maxMonth = Conf.getInt("shared.limits.max_month"),
    minGender = Conf.getInt("shared.limits.min_gender"),
    maxGender = Conf.getInt("shared.limits.max_gender"),
    minAge = Conf.getInt("shared.limits.min_age"),
    maxAge = Conf.getInt("shared.limits.max_age")
  )
  val dates = Dates(
    ageReference = makeTS(Conf.getIntList("dates.age_reference").asScala.toList)
  )
}
