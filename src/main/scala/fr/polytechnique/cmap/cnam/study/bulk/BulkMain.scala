package fr.polytechnique.cmap.cnam.study.bulk

import java.io.PrintWriter
import scala.collection.mutable
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{DcirMedicalActExtractor, McoCcamActExtractor, McoCeActExtractor, McoCimMedicalActExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.classifications.GhmExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.HospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}

object BulkMain extends Main {
  override def appName: String = "BulkMain"

  override def run(
    sqlContext: SQLContext,
    argsMap: Map[String, String]): Option[Dataset[_]] = {


    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()
    val bulkConfig = BulkConfig.load(argsMap("conf"), argsMap("env"))

    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(bulkConfig.input))

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val drugs = new DrugExtractor(bulkConfig.drugs).extract(sources, Set.empty).cache()

    operationsMetadata += {
      OperationReporter.report(
        "DrugPurchases",
        List("DCIR"),
        OperationTypes.Dispensations,
        drugs.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }

    drugs.unpersist()

    val hospitalStays = HospitalStaysExtractor.extract(sources, Set.empty).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "HospitalStays",
          List("MCO"),
          OperationTypes.HospitalStays,
          hospitalStays.toDF,
          Path(bulkConfig.output.outputSavePath),
          bulkConfig.output.saveMode
        )
    }

    val dcirMedicalAct = DcirMedicalActExtractor.extract(sources, Set.empty).cache()

    operationsMetadata += {
      OperationReporter.report(
        "DCIRMedicalAct",
        List("DCIR"),
        OperationTypes.MedicalActs,
        dcirMedicalAct.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }

    dcirMedicalAct.unpersist()


    val cimMedicalAct = McoCimMedicalActExtractor.extract(sources, Set.empty).cache()

    operationsMetadata += {
      OperationReporter.report(
        "CIM-Medical-Acts",
        List("MCO"),
        OperationTypes.MedicalActs,
        cimMedicalAct.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }

    cimMedicalAct.unpersist()


    val ccamMedicalAct = McoCcamActExtractor.extract(sources, Set.empty).cache()

    operationsMetadata += {
      OperationReporter.report(
        "CCAM-Medical-Acts",
        List("MCO"),
        OperationTypes.MedicalActs,
        ccamMedicalAct.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }

    ccamMedicalAct.unpersist()


    val liberalActs = McoCeActExtractor.extract(sources, Set.empty).cache()

    operationsMetadata += {
      OperationReporter.report(
        "McoCEMedicalActs",
        List("MCO_ACE"),
        OperationTypes.MedicalActs,
        liberalActs.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }

    liberalActs.unpersist()

    val imbActs = ImbDiagnosisExtractor.extract(sources, Set.empty).cache()

    operationsMetadata += {
      OperationReporter.report(
        "ImbDiagnoses",
        List("IR_IMB_R"),
        OperationTypes.MedicalActs,
        imbActs.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }

    imbActs.unpersist()

    val classification = GhmExtractor.extract(sources, Set.empty).cache()

    operationsMetadata += {
      OperationReporter.report(
        "GHM",
        List("MCO"),
        OperationTypes.AnyEvents,
        classification.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }

    classification.unpersist()

    val mainDiag = MainDiagnosisExtractor.extract(sources, Set.empty).cache()

    operationsMetadata += {
      OperationReporter.report(
        "MainDiagnosis",
        List("MCO"),
        OperationTypes.Diagnosis,
        mainDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    mainDiag.unpersist()

    val linkedDiag = LinkedDiagnosisExtractor.extract(sources, Set.empty).cache()

    operationsMetadata += {
      OperationReporter.report(
        "LinkedDiagnosis",
        List("MCO"),
        OperationTypes.Diagnosis,
        linkedDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    linkedDiag.unpersist()

    val associatedDiag = AssociatedDiagnosisExtractor.extract(sources, Set.empty).cache()
    operationsMetadata += {
      OperationReporter.report(
        "AssociatedDiagnosis",
        List("MCO"),
        OperationTypes.Diagnosis,
        associatedDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    associatedDiag.unpersist()


    val patients = new Patients(PatientsConfig(bulkConfig.base.studyStart)).extract(sources).cache()
    operationsMetadata += {
      OperationReporter.report(
        "BasePopulation",
        List("IR_BEN", "DCIR", "MCO", "MCO_CE"),
        OperationTypes.Patients,
        patients.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    patients.unpersist()

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata_bulk_" + format.format(startTimestamp) + ".json") {
      write(metadataJson)
      close()
    }

    None
  }
}
