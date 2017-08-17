package fr.polytechnique.cmap.cnam.study.pioglitazone

import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import fr.polytechnique.cmap.cnam.util.functions.makeTS
object PioglitazoneConfig {


  private lazy val conf: Config = {
    // This is a little hacky. In the future, it may be nice to find a better way.
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")
    ConfigFactory.parseResources("config/pio/pioglitazone.conf").resolve().getConfig(environment)
  }

  case class PioglitazoneParams(
    dcirMedicalActCodes: List[String] = List(),
    mcoCIM10MedicalActCodes: List[String] = List(),
    mcoCCAMMedicalActCodes: List[String] = List(),
    imbDiagnosisCodes: List[String] = List("C67"),
    ageReferenceDate: java.sql.Timestamp = makeTS(2006, 12, 31, 23, 59, 59),
    drugCategories: List[String] = List("A10"),
    lastDate: java.sql.Timestamp = makeTS(2009, 12, 31, 23, 59, 59),
    studyStart:  java.sql.Timestamp = makeTS(2006, 1, 1),
    studyEnd:  java.sql.Timestamp = makeTS(2009, 12, 31, 23, 59, 59),
    cancerDefinition: String = "broad",
    codesMapDP: List[String] = List("C67", "C77", "C78", "C79"),
    codesMapDR: List[String] = List("C67", "C77", "C78", "C79"),
    codesMapDA: List[String] = List("C67"),
    min_purchases: Int = 1, // 1 or 2
    start_delay: Int = 0, // can vary from 0 to 3
    purchases_window: Int = 0, // always 0
    only_first: Boolean = false, // can be always false and handled in python / C++, but not soon
    filter_never_sick_patients: Boolean = false, // always true
    filter_lost_patients: Boolean = false, //keep it
    filter_diagnosed_patients: Boolean = true, // keep it
    diagnosed_patients_threshold: Int = 6, // keep it, maybe remove the previous one and set false when this param is 0
    filter_delayed_entries: Boolean = true, // keep it
    delayed_entry_threshold: Int = 12 /* keep it, maybe remove the previous one and set false when this param is 0*/)
    extends CaseClassConfig

  lazy val pioglitazoneParameters = PioglitazoneParams(
    min_purchases = conf.getInt(""),
    start_delay = conf.getInt(""),
    purchases_window = conf.getInt(""),
    only_first = conf.getBoolean(""),
    filter_never_sick_patients = conf.getBoolean(""),
    filter_lost_patients = conf.getBoolean(""),
    filter_diagnosed_patients = conf.getBoolean(""),
    diagnosed_patients_threshold = conf.getInt(""),
    filter_delayed_entries = conf.getBoolean(""),
    delayed_entry_threshold = conf.getInt("")
  )
}