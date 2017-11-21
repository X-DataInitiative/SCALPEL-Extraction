package fr.polytechnique.cmap.cnam.study.pioglitazone

import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

object PioglitazoneConfig {

  private lazy val conf: Config = {
    // This is a little hacky. In the future, it may be nice to find a better way.
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")
    val defaultConfig = ConfigFactory.parseResources("config/pioglitazone/pioglitazone.conf").resolve().getConfig(environment)
    val newConfig = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()
    newConfig.withFallback(defaultConfig).resolve()
  }

  case class StudyParams(
    ageReferenceDate: java.sql.Timestamp = makeTS(2006, 12, 31, 23, 59, 59),
    studyStart: java.sql.Timestamp = makeTS(2006, 1, 1),
    studyEnd: java.sql.Timestamp = makeTS(2009, 12, 31, 23, 59, 59),
    lastDate: java.sql.Timestamp = makeTS(2009, 12, 31, 23, 59, 59),
    cancerDefinition: String = "broad",
    delayed_entry_threshold: Int = 12 /* keep it, maybe remove the previous one and set false when this param is 0*/)

  case class DiagnosesParams(
    codesMapDP: List[String] = List("C67", "C77", "C78", "C79"),
    codesMapDR: List[String] = List("C67", "C77", "C78", "C79"),
    codesMapDA: List[String] = List("C67"),
    imbDiagnosisCodes: List[String] = List("C67"))

  case class MedicalActParams(
    dcirMedicalActCodes: List[String] = List(),
    mcoCIM10MedicalActCodes: List[String] = List(),
    mcoCCAMMedicalActCodes: List[String] = List())

  case class FiltersParams(
    filter_never_sick_patients: Boolean = false, // always true
    filter_lost_patients: Boolean = false,
    filter_diagnosed_patients: Boolean = true,
    diagnosed_patients_threshold: Int = 6, // keep it, maybe remove the previous one and set false when this param is 0
    filter_delayed_entries: Boolean = true)

  case class DrugsParams(
    min_purchases: Int = 1, // 1 or 2
    start_delay: Int = 0, // can vary from 0 to 3
    purchases_window: Int = 0, // always 0
    only_first: Boolean = false /* can be always false and handled in python / C++, but not soon*/,
    drugCategories: List[String] = List("A10"))

  case class PioglitazoneParams(
    drugs: DrugsParams = DrugsParams(),
    medicalActs: MedicalActParams = MedicalActParams(),
    diagnoses: DiagnosesParams = DiagnosesParams(),
    study: StudyParams = StudyParams(),
    filters: FiltersParams = FiltersParams())
    extends CaseClassConfig


    lazy val pioglitazoneParameters = PioglitazoneParams(
      drugs = DrugsParams(min_purchases = conf.getInt("min_purchases"),
        start_delay = conf.getInt("start_delay"),
        only_first = conf.getBoolean("only_first"),
        purchases_window = conf.getInt("purchases_window")),
      filters = FiltersParams(
        filter_never_sick_patients = conf.getBoolean("filter_never_sick_patients"),
        filter_lost_patients = conf.getBoolean("filter_lost_patients"),
        filter_diagnosed_patients = conf.getBoolean("filter_diagnosed_patients"),
        diagnosed_patients_threshold = conf.getInt("diagnosed_patients_threshold"),
        filter_delayed_entries = conf.getBoolean("filter_delayed_entries")
       ),
      study = StudyParams(delayed_entry_threshold = conf.getInt("delayed_entry_threshold")))

}