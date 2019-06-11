package fr.polytechnique.cmap.cnam.study.fall

import java.io.PrintWriter
import scala.collection.mutable
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActs
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.Diagnoses
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugsExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.{HospitalStayConfig, HospitalStayExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.ExposuresTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.follow_up.FallStudyFollowUps
import fr.polytechnique.cmap.cnam.study.fall.fractures.FracturesTransformer
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}

object FallMain extends Main with FractureCodes {

  override def appName: String = "fall study"

  def computeHospitalStays(sources: Sources, fallConfig: FallConfig): mutable.Buffer[OperationMetadata] = {
    val operationsMetadata = mutable.Buffer[OperationMetadata]()
    if (fallConfig.runParameters.hospitalStays) {
      val hospitalStays = new HospitalStayExtractor(
        HospitalStayConfig(
          fallConfig.base.studyStart,
          fallConfig.base.studyEnd
        )
      ).extract(sources)
      operationsMetadata += {
        OperationReporter
          .report(
            "extract_hospital_stays",
            List("MCO"),
            OperationTypes.HospitalStays,
            hospitalStays.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
    }
    operationsMetadata
  }

  def computeExposures(sources: Sources, fallConfig: FallConfig): mutable.Buffer[OperationMetadata] = {

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val optionDrugPurchases = if (fallConfig.runParameters.drugPurchases) {
      val drugPurchases = new DrugsExtractor(fallConfig.drugs).extract(sources).cache()
      operationsMetadata += {
        OperationReporter
          .report(
            "drug_purchases",
            List("DCIR"),
            OperationTypes.Dispensations,
            drugPurchases.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      Some(drugPurchases)
    } else {
      None
    }

    val optionPatients = if (fallConfig.runParameters.patients) {
      val patients = new Patients(PatientsConfig(fallConfig.base.studyStart)).extract(sources).cache()
      operationsMetadata += {
        OperationReporter
          .report(
            "extract_patients",
            List("DCIR", "MCO", "IR_BEN_R", "MCO_CE"),
            OperationTypes.Patients,
            patients.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      Some(patients)
    } else {
      None
    }

    if (fallConfig.runParameters.startGapPatients) {
      import PatientFilters._
      val filteredPatients: Dataset[Patient] = optionPatients.get
        .filterNoStartGap(optionDrugPurchases.get, fallConfig.base.studyStart, fallConfig.patients.startGapInMonths)
      operationsMetadata += {
        OperationReporter
          .report(
            "filter_patients",
            List("drug_purchases", "extract_patients"),
            OperationTypes.Patients,
            filteredPatients.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
    }

    if (fallConfig.runParameters.exposures) {
      val exposures = {
        val patientsWithFollowUp = FallStudyFollowUps
          .transform(
            optionPatients.get,
            fallConfig.base.studyStart,
            fallConfig.base.studyEnd,
            fallConfig.patients.followupStartDelay
          )
        import patientsWithFollowUp.sparkSession.sqlContext.implicits._
        val followUps = patientsWithFollowUp.map(_._2)
        operationsMetadata += {
          OperationReporter
            .report(
              "follow_up",
              List("extract_patients"),
              OperationTypes.AnyEvents,
              followUps.toDF,
              Path(fallConfig.output.outputSavePath),
              fallConfig.output.saveMode
            )
        }
        val definition : FallConfig.ExposureConfig = fallConfig.exposures.copy(
          patients = Some(patientsWithFollowUp), dispensations = Some(optionDrugPurchases.get)
        )
        new ExposuresTransformer(definition).transform()
      }
      operationsMetadata += {
        OperationReporter
          .report(
            "exposures",
            List("drug_purchases"),
            OperationTypes.Exposures,
            exposures.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
    }

    operationsMetadata
  }

  def computeOutcomes(sources: Sources, fallConfig: FallConfig): mutable.Buffer[OperationMetadata] = {

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val optionDiagnoses = if (fallConfig.runParameters.diagnoses) {
      val diagnoses = new Diagnoses(fallConfig.diagnoses).extract(sources).persist()
      operationsMetadata += {
        OperationReporter
          .report(
            "diagnoses",
            List("MCO", "IR_IMB_R"),
            OperationTypes.Diagnosis,
            diagnoses.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      Some(diagnoses)
    } else {
      None
    }

    val (optionLiberalActs, optionActs) = if (fallConfig.runParameters.acts) {
      val acts = new MedicalActs(fallConfig.medicalActs).extract(sources).persist()
      operationsMetadata += {
        OperationReporter
          .report(
            "acts",
            List("DCIR", "MCO", "MCO_CE"),
            OperationTypes.MedicalActs,
            acts.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      val liberalActs = acts
        .filter(act => act.groupID == DcirAct.groupID.Liberal && !CCAMExceptions.contains(act.value)).persist()
      operationsMetadata += {
        OperationReporter
          .report(
            "liberal_acts",
            List("acts"),
            OperationTypes.MedicalActs,
            liberalActs.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      (Some(acts), Some(liberalActs))
    } else {
      (None, None)
    }

    if (fallConfig.runParameters.outcomes) {
      val fractures: Dataset[Event[Outcome]] = new FracturesTransformer(fallConfig)
        .transform(optionLiberalActs.get, optionActs.get, optionDiagnoses.get)
      operationsMetadata += {
        OperationReporter
          .report(
            "fractures",
            List("acts"),
            OperationTypes.Outcomes,
            fractures.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
    }

    operationsMetadata
  }

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()
    val fallConfig = FallConfig.load(argsMap("conf"), argsMap("env"))

    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input))
    val dcir = sources.dcir.get.repartition(4000).persist()
    val mco = sources.mco.get.repartition(4000).persist()

    val operationsMetadata = computeHospitalStays(sources, fallConfig) ++ computeOutcomes(
      sources,
      fallConfig
    ) ++ computeExposures(sources, fallConfig)

    dcir.unpersist()
    mco.unpersist()


    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata_fall_" + format.format(startTimestamp) + ".json") {
      write(metadataJson)
      close()
    }

    None
  }
}
