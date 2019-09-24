package fr.polytechnique.cmap.cnam.study.dreesChronic

import scala.collection.mutable
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
//import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, Outcome}
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.McoHospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.dreesChronic.codes._
import fr.polytechnique.cmap.cnam.study.dreesChronic.config.DreesChronicConfig
import fr.polytechnique.cmap.cnam.study.dreesChronic.extractors._
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}

object dreesChronicMain extends Main with BpcoCodes {

  override def appName: String = "dreesChronic study"

  def computeHospitalStays(sources: Sources, dreesChronicConfig: DreesChronicConfig): mutable.Buffer[OperationMetadata] = {
    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    if (dreesChronicConfig.runParameters.hospitalStays) {

      val hospitalStays = new HospitalStaysExtractor().extract(sources).cache()

      operationsMetadata += {
        OperationReporter
          .report(
            "extract_hospital_stays",
            List("MCO", "SSR_SEJ", "HAD"),
            OperationTypes.HospitalStays,
            hospitalStays.toDF,
            Path(dreesChronicConfig.output.outputSavePath),
            dreesChronicConfig.output.saveMode
          )
      }
    }
    operationsMetadata
  }

  def computeExposures(sources: Sources, dreesChronicConfig: DreesChronicConfig): mutable.Buffer[OperationMetadata] = {

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val optionDrugPurchases = if (dreesChronicConfig.runParameters.drugPurchases) {

      val drugPurchases = new DrugsExtractor(dreesChronicConfig.drugs).extract(sources).cache()

      operationsMetadata += {
        OperationReporter
          .report(
            "drug_purchases",
            List("DCIR"),
            OperationTypes.Dispensations,
            drugPurchases.toDF,
            Path(dreesChronicConfig.output.outputSavePath),
            dreesChronicConfig.output.saveMode
          )
      }
      Some(drugPurchases)
    } else {
      None
    }

    val optionPatients = if (dreesChronicConfig.runParameters.patients) {

      val patients = new Patients(PatientsConfig(dreesChronicConfig.base.studyStart)).extract(sources).cache()

      operationsMetadata += {
        OperationReporter.report(
          "extract_patients",
          List("DCIR", "MCO", "SSR_SEJ", "HAD", "IR_BEN_R", "MCO_CE"),
          OperationTypes.Patients,
          patients.toDF,
          Path(dreesChronicConfig.output.outputSavePath),
          dreesChronicConfig.output.saveMode)
      }
      Some(patients)
    } else {
      None
    }

    if (dreesChronicConfig.runParameters.startGapPatients) {

      import PatientFilters._

      val filteredPatients: Dataset[Patient] = optionPatients.get
        .filterNoStartGap(
          optionDrugPurchases.get,
          dreesChronicConfig.base.studyStart,
          dreesChronicConfig.patients.startGapInMonths
        )

      operationsMetadata += {
        OperationReporter
          .report(
            "filter_patients",
            List("drug_purchases", "extract_patients"),
            OperationTypes.Patients,
            filteredPatients.toDF,
            Path(dreesChronicConfig.output.outputSavePath),
            dreesChronicConfig.output.saveMode
          )
      }
    }

    operationsMetadata
  }

  def computePrestations(sources: Sources, dreesChronicConfig: DreesChronicConfig): mutable.Buffer[OperationMetadata] = {

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    if (dreesChronicConfig.runParameters.practionnerClaimSpeciality) {

      val prestations =  new PractitionnerClaimSpecialityExtractor(dreesChronicConfig.practionnerClaimSpeciality).extract(sources).cache()

      operationsMetadata += {
        OperationReporter.report(
          "practionner_specialities",
          List("DCIR"),
          OperationTypes.PractitionnerClaimSpecialities,
          prestations.toDF,
          Path(dreesChronicConfig.output.outputSavePath),
          dreesChronicConfig.output.saveMode
        )
      }
      Some(prestations)
    } else {
      None
    }

    val optionNgap = if (dreesChronicConfig.runParameters.ngapActs) {

      val ngapActs = new DcirNgapActsExtractor(dreesChronicConfig.ngapActs).extract(sources).persist()

      operationsMetadata += {
        OperationReporter.report(
          "ngapActs",
          List("DCIR"),
          OperationTypes.NgapActs,
          ngapActs.toDF,
          Path(dreesChronicConfig.output.outputSavePath),
          dreesChronicConfig.output.saveMode
        )
      }
      Some(ngapActs)
    } else None

    operationsMetadata
  }

  def computeOutcomes(sources: Sources, dreesChronicConfig: DreesChronicConfig): mutable.Buffer[OperationMetadata] = {

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    if (dreesChronicConfig.runParameters.diagnoses) {

      val diagnoses = new DiagnosisExtractor(dreesChronicConfig.diagnoses).extract(sources).cache()

      operationsMetadata += {
        OperationReporter.report(
          "diagnoses",
          List("MCO", "SSR_SEJ", "HAD"),
          OperationTypes.Diagnosis,
          diagnoses.toDF,
          Path(dreesChronicConfig.output.outputSavePath),
          dreesChronicConfig.output.saveMode
        )
      }
      Some(diagnoses)
    } else {
      None
    }

    if (dreesChronicConfig.runParameters.acts) {

      val acts = new ActsExtractor(dreesChronicConfig.medicalActs).extract(sources).persist()

      operationsMetadata += {
        OperationReporter.report(
          "acts",
          List("DCIR", "MCO", "MCO_CE", "SSR_SEJ", "HAD"),
          OperationTypes.MedicalActs,
          acts.toDF,
          Path(dreesChronicConfig.output.outputSavePath),
          dreesChronicConfig.output.saveMode
        )
      }
      Some(acts)
    } else {
      None
    }

    operationsMetadata
  }

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()

    val dreesChronicConfig = DreesChronicConfig.load(argsMap("conf"), argsMap("env"))

    import implicits.SourceReader
    val sourcesAllYears = Sources.sanitize(sqlContext.readSources(dreesChronicConfig.input))
    val sources = Sources.sanitizeDates(sourcesAllYears, dreesChronicConfig.base.studyStart, dreesChronicConfig.base.studyEnd)
    //val dcir = sources.dcir.get.repartition(4000).persist()
    //val mco = sources.mco.get.repartition(4000).persist()
    //val ssr = sources.ssr.get.repartition(4000).persist()

    val operationsMetadata = computeHospitalStays(sources, dreesChronicConfig) ++ computePrestations(sources, dreesChronicConfig) ++  computeOutcomes(sources, dreesChronicConfig) ++ computeExposures(sources, dreesChronicConfig)

    //dcir.unpersist()
    //mco.unpersist()
    //ssr.unpersist()


    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    OperationReporter.writeMetaData(metadataJson, "metadata_dreesChronic_" + format.format(startTimestamp) + ".json", argsMap("env"))

    None
  }
}
