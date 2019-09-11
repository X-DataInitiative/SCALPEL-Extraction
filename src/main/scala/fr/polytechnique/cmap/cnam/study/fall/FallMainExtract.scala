package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events.DcirAct
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.HospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.extractors._
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.reporting._
import org.apache.spark.sql.{Dataset, SQLContext}

import scala.collection.mutable

object FallMainExtract extends Main with FractureCodes {

  override def appName: String = "fall study extract"

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    import implicits.SourceReader
    val fallConfig = FallConfig.load(argsMap("conf"), argsMap("env"))
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input))
    val dcir = sources.dcir.get.repartition(4000).persist()
    val mco = sources.mco.get.repartition(4000).persist()
    val meta = mutable.HashMap[String, OperationMetadata]()
    computeHospitalStays(meta, sources, fallConfig)
    computeOutcomes(meta, sources, fallConfig)
    computeExposures(meta, sources, fallConfig)
    OperationMetadata.serialize(argsMap("meta_bin"), meta)
    dcir.unpersist()
    mco.unpersist()
    None
  }

  def computeHospitalStays(meta: mutable.HashMap[String, OperationMetadata], sources: Sources, fallConfig: FallConfig):
  mutable.HashMap[String, OperationMetadata] = {

    if (fallConfig.runParameters.hospitalStays) {
      val hospitalStays = HospitalStaysExtractor.extract(sources, Set.empty).cache()
      meta += {
        "extract_hospital_stays" ->
          OperationReporter
            .reportAsDataSet(
              "extract_hospital_stays",
              List("MCO"),
              OperationTypes.HospitalStays,
              hospitalStays,
              Path(fallConfig.output.outputSavePath),
              fallConfig.output.saveMode
            )
      }
    }
    meta
  }

  def computeExposures(meta: mutable.HashMap[String, OperationMetadata], sources: Sources, fallConfig: FallConfig):
  mutable.HashMap[String, OperationMetadata] = {

    if (fallConfig.runParameters.drugPurchases) {
      val drugPurchases = new DrugsExtractor(fallConfig.drugs).extract(sources).cache()
      meta += {
        "drug_purchases" ->
          OperationReporter
            .reportAsDataSet(
              "drug_purchases",
              List("DCIR"),
              OperationTypes.Dispensations,
              drugPurchases,
              Path(fallConfig.output.outputSavePath),
              fallConfig.output.saveMode
            )
      }
    }

    if (fallConfig.runParameters.patients) {
      val patients: Dataset[Patient] = new Patients(PatientsConfig(fallConfig.base.studyStart)).extract(sources).cache()
      meta += {
        "extract_patients" ->
          OperationReporter
            .reportAsDataSet(
              "extract_patients",
              List("DCIR", "MCO", "IR_BEN_R", "MCO_CE"),
              OperationTypes.Patients,
              patients,
              Path(fallConfig.output.outputSavePath),
              fallConfig.output.saveMode
            )
      }
    }
    meta
  }

  def computeOutcomes(meta: mutable.HashMap[String, OperationMetadata], sources: Sources, fallConfig: FallConfig):
  mutable.HashMap[String, OperationMetadata] = {

    if (fallConfig.runParameters.diagnoses) {
      logger.info("diagnoses")
      val diagnoses = new DiagnosisExtractor(fallConfig.diagnoses).extract(sources).persist()
      val diagnoses_report = OperationReporter.reportAsDataSet(
        "diagnoses",
        List("MCO", "IR_IMB_R"),
        OperationTypes.Diagnosis,
        diagnoses,
        Path(fallConfig.output.outputSavePath),
        fallConfig.output.saveMode
      )
      meta += {
        diagnoses_report.name -> diagnoses_report
      }
    }

    if (fallConfig.runParameters.acts) {
      logger.info("Medical Acts")
      val acts = new ActsExtractor(fallConfig.medicalActs).extract(sources).persist()
      val acts_report = OperationReporter.reportAsDataSet(
        "acts",
        List("DCIR", "MCO", "MCO_CE"),
        OperationTypes.MedicalActs,
        acts,
        Path(fallConfig.output.outputSavePath),
        fallConfig.output.saveMode
      )
      meta += {
        acts_report.name -> acts_report
      }
      logger.info("Liberal Medical Acts")
      val liberalActs = acts
        .filter(act => act.groupID == DcirAct.groupID.Liberal && !CCAMExceptions.contains(act.value)).persist()
      val liberal_acts_report = OperationReporter.reportAsDataSet(
        "liberal_acts",
        List("acts"),
        OperationTypes.MedicalActs,
        liberalActs,
        Path(fallConfig.output.outputSavePath),
        fallConfig.output.saveMode
      )
      meta += {
        liberal_acts_report.name -> liberal_acts_report
      }
    }
    meta
  }
}
