package fr.polytechnique.cmap.cnam.study.bulk

import java.io.PrintWriter

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{DcirMedicalActExtractor, HadCcamActExtractor, McoCcamActExtractor, McoCeActExtractor, McoCimMedicalActExtractor, SsrCcamActExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.classifications.GhmExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.{HadHospitalStaysExtractor, McoHospitalStaysExtractor, SsrHospitalStaysExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.DcirNgapActExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.takeOverReasons.HadMainTakeOverExtractor
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.dreesChronic.extractors.PractitionnerClaimSpecialityExtractor
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}
import org.apache.spark.sql.{Dataset, SQLContext}

import scala.collection.mutable

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

    val hadHospitalStays = HadHospitalStaysExtractor.extract(sources, Set.empty)//.cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "HadHospitalStays",
          List("HAD"),
          OperationTypes.HospitalStays,
          hadHospitalStays.toDF,
          Path(bulkConfig.output.outputSavePath),
          bulkConfig.output.saveMode
        )
    }
    //hadHospitalStays.unpersist()

    val ssrMainDiag = SsrMainDiagnosisExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "SsrMainDiagnosis",
        List("SSR"),
        OperationTypes.Diagnosis,
        ssrMainDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //ssrMainDiag.unpersist()

    val ssrLinkedDiag = SsrLinkedDiagnosisExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "SsrLinkedDiagnosis",
        List("SSR"),
        OperationTypes.Diagnosis,
        ssrLinkedDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //ssrLinkedDiag.unpersist()

    val ssrAssociatedDiag = SsrAssociatedDiagnosisExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "SsrAssociatedDiagnosis",
        List("SSR"),
        OperationTypes.Diagnosis,
        ssrAssociatedDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //ssrAssociatedDiag.unpersist()

    val ssrTakingOverPurpose = SsrTakingOverPurposeExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "SsrTakingOverPurpose",
        List("SSR"),
        OperationTypes.Diagnosis,
        ssrTakingOverPurpose.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //ssrTakingOverPurpose.unpersist()

    val hadMainDiag = HadMainDiagnosisExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "HadMainDiagnosis",
        List("HAD"),
        OperationTypes.Diagnosis,
        hadMainDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //hadMainDiag.unpersist()

    val hadAssociatedDiag = HadAssociatedDiagnosisExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "HadAssociatedDiagnosis",
        List("HAD"),
        OperationTypes.Diagnosis,
        hadAssociatedDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //hadAssociatedDiag.unpersist()

    val hadMainTakeOverReason = HadMainTakeOverExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "HadMainTakeOverReason",
        List("HAD"),
        OperationTypes.Diagnosis,
        hadMainTakeOverReason.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //hadMainTakeOverReason.unpersist()

    val ssrCcamMedicalActs = SsrCcamActExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "SsrCcamMedicalActs",
        List("SSR"),
        OperationTypes.MedicalActs,
        ssrCcamMedicalActs.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //ssrCcamMedicalActs.unpersist()

//    val ssrCsarrMedicalActs = SsrCsarrActExtractor.extract(sources, Set.empty)//.cache()
//
//    operationsMetadata += {
//      OperationReporter.report(
//        "SsrCsarrMedicalActs",
//        List("SSR"),
//        OperationTypes.MedicalActs,
//        ssrCsarrMedicalActs.toDF,
//        Path(bulkConfig.output.outputSavePath),
//        bulkConfig.output.saveMode
//      )
//    }
//    //ssrCsarrMedicalActs.unpersist()

    val hadCcamMedicalActs = HadCcamActExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "HadCcamMedicalActs",
        List("HAD"),
        OperationTypes.MedicalActs,
        hadCcamMedicalActs.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //hadCcamMedicalActs.unpersist()

    val mcoHospitalStays = McoHospitalStaysExtractor.extract(sources, Set.empty)//.cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "McoHospitalStays",
          List("MCO"),
          OperationTypes.HospitalStays,
          mcoHospitalStays.toDF,
          Path(bulkConfig.output.outputSavePath),
          bulkConfig.output.saveMode
        )
    }
    //mcoHospitalStays.unpersist()


    val ssrHospitalStays = SsrHospitalStaysExtractor.extract(sources, Set.empty)//.cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "SsrHospitalStays",
          List("SSR"),
          OperationTypes.HospitalStays,
          ssrHospitalStays.toDF,
          Path(bulkConfig.output.outputSavePath),
          bulkConfig.output.saveMode
        )
    }
    //ssrHospitalStays.unpersist()


    val drugs = new DrugExtractor(bulkConfig.drugs).extract(sources, Set.empty)//.cache()

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
    //drugs.unpersist()

    val prestations = new PractitionnerClaimSpecialityExtractor(bulkConfig.practionnerClaimSpeciality).extract(sources)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "practionner_specialities",
        List("DCIR"),
        OperationTypes.PractitionnerClaimSpecialities,
        prestations.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //prestations.unpersist()

    val dcirMedicalAct = DcirMedicalActExtractor.extract(sources, Set.empty)//.cache()
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
    //dcirMedicalAct.unpersist()

    //val dcirNgapAct = new DcirNgapActExtractor(BulkConfig.ngapActs).extract(sources)//.cache()

    val McoCimMedicalAct = McoCimMedicalActExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "McoCIM-Medical-Acts",
        List("MCO"),
        OperationTypes.MedicalActs,
        McoCimMedicalAct.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //McoCimMedicalAct.unpersist()


    val mcoCcamMedicalAct = McoCcamActExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "McoCCAM-Medical-Acts",
        List("MCO"),
        OperationTypes.MedicalActs,
        mcoCcamMedicalAct.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //mcoCcamMedicalAct.unpersist()


    val liberalActs = McoCeActExtractor.extract(sources, Set.empty)//.cache()

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
    //liberalActs.unpersist()

    val imbDiagnoses = ImbDiagnosisExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "ImbDiagnoses",
        List("IR_IMB_R"),
        OperationTypes.MedicalActs,
        imbDiagnoses.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //imbDiagnoses.unpersist()

    val classification = GhmExtractor.extract(sources, Set.empty)//.cache()

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
    //classification.unpersist()

    val mcoMainDiag = McoMainDiagnosisExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "McoMainDiagnosis",
        List("MCO"),
        OperationTypes.Diagnosis,
        mcoMainDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //mcoMainDiag.unpersist()

    val mcoLinkedDiag = McoLinkedDiagnosisExtractor.extract(sources, Set.empty)//.cache()

    operationsMetadata += {
      OperationReporter.report(
        "McoLinkedDiagnosis",
        List("MCO"),
        OperationTypes.Diagnosis,
        mcoLinkedDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //mcoLinkedDiag.unpersist()

    val mcoAssociatedDiag = McoAssociatedDiagnosisExtractor.extract(sources, Set.empty)//.cache()
    operationsMetadata += {
      OperationReporter.report(
        "McoAssociatedDiagnosis",
        List("MCO"),
        OperationTypes.Diagnosis,
        mcoAssociatedDiag.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //mcoAssociatedDiag.unpersist()

    val patients = new Patients(PatientsConfig(bulkConfig.base.studyStart)).extract(sources)//.cache()
    operationsMetadata += {
      OperationReporter.report(
        "BasePopulation",
        List("IR_BEN", "DCIR", "MCO", "MCO_CE", "SSR", "HAD"),
        OperationTypes.Patients,
        patients.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    //patients.unpersist()

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
