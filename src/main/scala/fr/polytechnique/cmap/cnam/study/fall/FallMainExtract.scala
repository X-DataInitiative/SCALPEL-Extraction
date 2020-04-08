// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall

import scala.collection.mutable
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events.DcirAct
import fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays.McoHospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.extractors._
import fr.polytechnique.cmap.cnam.study.fall.statistics.DiagnosisCounter
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.reporting._

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
    computeControls(meta, sources, fallConfig)
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
      val hospitalStays = McoHospitalStaysExtractor.extract(sources).cache()
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
      val controlDrugPurchases = ControlDrugs.extract(sources).cache()
      meta += {
        "control_drugs_purchases" ->
          OperationReporter
            .reportAsDataSet(
              "control_drugs_purchases",
              List("DCIR"),
              OperationTypes.Dispensations,
              controlDrugPurchases,
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
      val diagnosesPopulation = DiagnosisCounter.process(diagnoses)
      val diagnoses_report = OperationReporter.reportDataAndPopulationAsDataSet(
        "diagnoses",
        List("MCO", "IR_IMB_R"),
        OperationTypes.Diagnosis,
        diagnoses,
        diagnosesPopulation,
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

  def computeControls(
    meta: mutable.HashMap[String, OperationMetadata],
    sources: Sources,
    fallConfig: FallConfig): mutable.Buffer[OperationMetadata] = {
    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val opioids = OpioidsExtractor.extract(sources).cache()
    meta += {
      "opioids" -> {
        OperationReporter
          .report(
            "opioids",
            List("DCIR"),
            OperationTypes.Dispensations,
            opioids.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
    }

    val ipp = IPPExtractor.extract(sources).cache()
    meta += {
      "IPP" -> OperationReporter
        .report(
          "IPP",
          List("DCIR"),
          OperationTypes.Dispensations,
          ipp.toDF,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
    }

    val cardiac = CardiacExtractor.extract(sources).cache()
    meta += {
      "cardiac" -> OperationReporter
        .report(
          "cardiac",
          List("DCIR"),
          OperationTypes.Dispensations,
          cardiac.toDF,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
    }

    val epileptics = EpilepticsExtractor.extract(sources).cache()
    meta += {
      "epileptics" ->
        OperationReporter
          .report(
            "epileptics",
            List("MCO", "IMB"),
            OperationTypes.Diagnosis,
            epileptics.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
    }

    val hta = HTAExtractor.extract(sources).cache()
    meta += {
      "HTA" ->
        OperationReporter
          .report(
            "HTA",
            List("DCIR"),
            OperationTypes.Dispensations,
            hta.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
    }
    operationsMetadata
  }
}
