package fr.polytechnique.cmap.cnam.study.rosiglitazone

import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object RosiglitazoneConfig {

  private lazy val conf: Config = {
    // This is a little hacky. In the future, it may be nice to find a better way.
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")
    ConfigFactory.parseResources("config/rosiglitazone/rosiglitazone.conf").resolve().getConfig(environment)
  }

  case class StudyParams(
    ageReferenceDate: java.sql.Timestamp = makeTS(2009, 12, 31, 23, 59, 59),
    studyStart: java.sql.Timestamp = makeTS(2009, 1, 1),
    studyEnd: java.sql.Timestamp = makeTS(2010, 12, 31, 23, 59, 59),
    lastDate: java.sql.Timestamp = makeTS(2010, 12, 31, 23, 59, 59),
    heartProblemDefinition: String = "infarctus",
    delayed_entry_threshold: Int = 12 /* keep it, maybe remove the previous one and set false when this param is 0*/
  )

  case class FiltersParams(
    filter_never_sick_patients: Boolean = false, // always true
    filter_lost_patients: Boolean = false,
    filter_diagnosed_patients: Boolean = true,
    diagnosed_patients_threshold: Int = 6, // keep it, maybe remove the previous one and set false when this param is 0
    filter_delayed_entries: Boolean = true
  )

  case class DrugsParams(
    min_purchases: Int = 1, // 1 or 2
    start_delay: Int = 0, // can vary from 0 to 3
    purchases_window: Int = 0, // always 0
    only_first: Boolean = false /* can be always false and handled in python / C++, but not soon*/ ,
    drugCategories: List[String] = List("A10")
  )

  case class RosiglitazoneParams(
   drugs: DrugsParams = DrugsParams(),
   study: StudyParams = StudyParams(),
   filters: FiltersParams = FiltersParams()
  ) extends CaseClassConfig

  lazy val rosiglitazoneParameters = RosiglitazoneParams(
    drugs = DrugsParams(
      min_purchases = conf.getInt("min_purchases"),
      start_delay = conf.getInt("start_delay"),
      only_first = conf.getBoolean("only_first"),
      purchases_window = conf.getInt("purchases_window")
    ),
    filters = FiltersParams(
      filter_never_sick_patients = conf.getBoolean("filter_never_sick_patients"),
      filter_lost_patients = conf.getBoolean("filter_lost_patients"),
      filter_diagnosed_patients = conf.getBoolean("filter_diagnosed_patients"),
      diagnosed_patients_threshold = conf.getInt("diagnosed_patients_threshold"),
      filter_delayed_entries = conf.getBoolean("filter_delayed_entries")
    ),
    study = StudyParams(delayed_entry_threshold = conf.getInt("delayed_entry_threshold")))
}
