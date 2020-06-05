// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.rosiglitazone

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays.McoHospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.events.molecules.MoleculePurchases
import fr.polytechnique.cmap.cnam.etl.extractors.patients.AllPatientExtractor
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.ExposureTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.patients.PatientFilters
import fr.polytechnique.cmap.cnam.etl.transformers.tracklosses.{TracklossTransformer, TracklossesConfig}
import fr.polytechnique.cmap.cnam.study.rosiglitazone.extractors.Diagnoses
import fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes.RosiglitazoneOutcomeTransformer
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}

object RosiglitazoneMain extends Main {

  val appName = "Rosiglitazone"

  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[_]] = {

    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()
    import sqlContext.implicits._
    import PatientFilters._

    val config = RosiglitazoneConfig.load(argsMap.getOrElse("conf", ""), argsMap("env"))

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    logger.info("Reading sources")
    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(config.input,config.fileFormat))

    //Extracting Patients
    val rawpatients: Dataset[Patient] = AllPatientExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter.report(
        "extract_raw_patients",
        List("DCIR", "MCO", "IR_BEN_R"),
        OperationTypes.Patients,
        rawpatients.toDF(),
        Path(config.output.outputSavePath),
        config.output.saveMode
      )
    }

    //Extracting Patients
    val patients: Dataset[Patient] = new PatientFilters(config.patients).filterPatients(rawpatients).cache()
    operationsMetadata += {
      OperationReporter.report(
        "extract_filtered_patients",
        List("DCIR", "MCO", "IR_BEN_R"),
        OperationTypes.Patients,
        patients.toDF(),
        Path(config.output.outputSavePath),
        config.output.saveMode
      )
    }

    // Extract Drug purchases
    val drugPurchases: Dataset[Event[Molecule]] = new MoleculePurchases(config.molecules).extract(sources).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "drug_purchases",
          List("DCIR"),
          OperationTypes.Dispensations,
          drugPurchases.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

    // Extract Diagnoses
    val diagnoses: Dataset[Event[Diagnosis]] = new Diagnoses(config.diagnoses).extract(sources).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "diagnoses",
          List("MCO", "IR_IMB_R"),
          OperationTypes.Diagnosis,
          diagnoses.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

    val hospitalStays = McoHospitalStaysExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter.report(
        "extract_hospital_stays",
        List("MCO"),
        OperationTypes.HospitalStays,
        hospitalStays.toDF,
        Path(config.output.outputSavePath),
        config.output.saveMode
      )
    }

    // Extract Heart Problems Outcomes
    val outcomes = {
      val outcomesTransformer = new RosiglitazoneOutcomeTransformer(config.outcomes.outcomeDefinition)
      outcomesTransformer.transform(diagnoses).cache()
    }
    operationsMetadata += {
      OperationReporter
        .report(
          "outcomes",
          List("diagnoses"),
          OperationTypes.Outcomes,
          outcomes.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

    // Extract Follow-up
    val followups = {
      //Extract Trackloss
      val tracklosses = {
        val tracklossConfig = TracklossesConfig(studyEnd = config.base.studyEnd)
        new TracklossTransformer(tracklossConfig).transform(drugPurchases).cache()
      }
      operationsMetadata += {
        OperationReporter
          .report(
            "trackloss",
            List("DCIR"),
            OperationTypes.AnyEvents,
            tracklosses.toDF,
            Path(config.output.outputSavePath),
            config.output.saveMode
          )
      }

      val observations = {
        val allEvents: Dataset[Event[AnyEvent]] = unionDatasets(
          drugPurchases.as[Event[AnyEvent]],
          diagnoses.as[Event[AnyEvent]]
        )
        new ObservationPeriodTransformer(config.observationPeriod).transform(allEvents).cache()
      }

      val patientsWithObservations = patients
        .joinWith(observations, patients.col("patientId") === observations.col("patientId"))

      val cachedFollowups = new FollowUpTransformer(config.followUp)
        .transform(patientsWithObservations, drugPurchases, outcomes, tracklosses)
        .cache()

      operationsMetadata += {
        OperationReporter
          .report(
            "followup",
            List("drug_purchases", "outcomes", "trackloss"),
            OperationTypes.AnyEvents,
            cachedFollowups.toDF,
            Path(config.output.outputSavePath),
            config.output.saveMode
          )
      }

      //unpersist
      tracklosses.unpersist()
      observations.unpersist()

      cachedFollowups
    }

    // Extract Filtering Patients
    val filteredPatientsAncestors = new ListBuffer[String]
    val filteredPatients = {
      val firstFilterResult = if (config.filters.filterDelayedEntries) {
        filteredPatientsAncestors += "drug_purchases"
        val delayedFreePatients = patients
          .filterDelayedPatients(drugPurchases, config.base.studyStart, config.filters.delayedEntryThreshold)
        operationsMetadata += {
          OperationReporter
            .report(
              "delayed_patients_free",
              List("drug_purchases"),
              OperationTypes.Patients,
              delayedFreePatients.toDF,
              Path(config.output.outputSavePath),
              config.output.saveMode
            )
        }
        delayedFreePatients
      }
      else {
        patients
      }

      if (config.filters.filterDiagnosedPatients) {
        filteredPatientsAncestors ++= List("outcomes", "followup")
        firstFilterResult.removeEarlyDiagnosedPatients(outcomes, followups, config.outcomes.outcomeDefinition.toString)
      }
      else {
        firstFilterResult
      }
    }
    operationsMetadata += {
      OperationReporter
        .report(
          "filtered_patients",
          filteredPatientsAncestors.toList,
          OperationTypes.Patients,
          filteredPatients.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

    // Extract Exposures
    val exposures = {
      val patientsWithFollowups = patients.joinWith(followups, followups.col("patientId") === patients.col("patientId"))
      new ExposureTransformer(config.exposures)
        .transform(patientsWithFollowups.map(_._2))(
          drugPurchases
            .map(m => Drug(m.patientID, m.groupID, m.value, m.weight, m.start, m.end))
        )
    }
    operationsMetadata += {
      OperationReporter
        .report(
          "exposures",
          List("drug_purchases", "followup"),
          OperationTypes.Exposures,
          exposures.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    OperationReporter
      .writeMetaData(metadataJson, "metadata_rosiglitazone_" + format.format(startTimestamp) + ".json", argsMap("env"))

    //unpersist
    patients.unpersist()
    drugPurchases.unpersist()
    diagnoses.unpersist()
    outcomes.unpersist()
    followups.unpersist()

    Some(exposures)
  }
}
