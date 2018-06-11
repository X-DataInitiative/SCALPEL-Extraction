package fr.polytechnique.cmap.cnam.study.fall

import java.io.PrintWriter
import scala.collection.mutable
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, Outcome}
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActs
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.Diagnoses
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugsExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.ExposuresTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.follow_up.FallStudyFollowUps
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}

object FallMain extends Main with FractureCodes {

  override def appName: String = "fall study"

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    val startTimestamp = new java.util.Date()
    val fallConfig = FallConfig.load(argsMap("conf"), argsMap("env"))

    logger.info("Reading sources")
    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input))
    val dcir = sources.dcir.get.repartition(4000).persist()
    val mco = sources.mco.get.repartition(4000).persist()

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    // Extract Drug purchases
    val drugPurchases = new DrugsExtractor(fallConfig.drugs).extract(sources).cache()
    operationsMetadata += {
      OperationReporter.report("drug_purchases", List("DCIR"), OperationTypes.Dispensations, drugPurchases.toDF, Path(fallConfig.output.root))
    }

    // Extract Patients
    val patients = new Patients(PatientsConfig(fallConfig.base.studyStart)).extract(sources).cache()
    operationsMetadata += {
      OperationReporter.report("extract_patients", List("DCIR", "MCO", "IR_BEN_R"), OperationTypes.Patients, patients.toDF, Path(fallConfig.output.patients))
    }

    // Filter Patients
    import PatientFilters._
    val filteredPatients: Dataset[Patient] = patients.filterNoStartGap(drugPurchases, fallConfig.base.studyStart, fallConfig.patients.startGapInMonths)
    operationsMetadata += {
      OperationReporter.report("filter_patients", List("drug_purchases", "extract_patients"), OperationTypes.Patients, filteredPatients.toDF, Path(fallConfig.output.root))
    }
    patients.unpersist()

    // Exposures
    val exposures = {
      val definition = fallConfig.exposures
      val patientsWithFollowUp = FallStudyFollowUps.transform(patients, fallConfig.base.studyStart, fallConfig.base.studyEnd, fallConfig.patients.followupStartDelay)
      new ExposuresTransformer(definition).transform(patientsWithFollowUp, drugPurchases)
    }
    operationsMetadata += {
      OperationReporter.report("exposures", List("drug_purchases"), OperationTypes.Exposures, exposures.toDF, Path(fallConfig.output.exposures))
    }
    drugPurchases.unpersist()

    // Medical Acts
    val acts = new MedicalActs(fallConfig.medicalActs).extract(sources).persist()
    operationsMetadata += {
      OperationReporter.report("acts", List("DCIR", "MCO", "MCO_CE"), OperationTypes.MedicalActs, acts.toDF, Path(fallConfig.output.root))
    }

    dcir.unpersist()

    // Diagnoses
    val diagnoses = new Diagnoses(fallConfig.diagnoses).extract(sources).persist()
    operationsMetadata += {
      OperationReporter.report("diagnoses", List("MCO", "IR_IMB_R"), OperationTypes.Diagnosis, diagnoses.toDF, Path(fallConfig.output.root))
    }
    mco.unpersist()

    // Liberal Medical Acts
    val liberalActs = acts.filter(act => act.groupID == DcirAct.groupID.Liberal && !CCAMExceptions.contains(act.value)).persist()
    operationsMetadata += {
      OperationReporter.report("liberal_acts", List("acts"), OperationTypes.MedicalActs, liberalActs.toDF, Path(fallConfig.output.root))
    }

    // fractures from All sources
    val fractures: Dataset[Event[Outcome]] = new FracturesTransformer(fallConfig.outcomes).transform(liberalActs, acts)
    operationsMetadata += {
      OperationReporter.report("fractures", List("acts"), OperationTypes.Outcomes, fractures.toDF, Path(fallConfig.output.outcomes))
    }
    diagnoses.unpersist()
    liberalActs.unpersist()
    acts.unpersist()

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata.json") {
      write(metadataJson)
      close()
    }

    None
  }
}
