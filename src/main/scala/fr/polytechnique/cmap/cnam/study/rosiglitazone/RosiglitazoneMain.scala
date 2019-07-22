package fr.polytechnique.cmap.cnam.study.rosiglitazone

import me.danielpes.spark.datetime.Period
import java.io.PrintWriter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.transformers.observation._
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up._
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Diagnosis, Event, Molecule}
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.HospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.molecules.MoleculePurchases
import fr.polytechnique.cmap.cnam.etl.extractors.patients.Patients
import fr.polytechnique.cmap.cnam.etl.extractors.tracklosses.{Tracklosses, TracklossesConfig}
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposuresTransformer, ExposuresTransformerConfig}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformer
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
    val sources = Sources.sanitize(sqlContext.readSources(config.input))

    //Extracting Patients
    val patients: Dataset[Patient] = new Patients(config.patients).extract(sources).cache()
    operationsMetadata += {
      OperationReporter.report(
        "extract_patients",
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

    val hospitalStays = HospitalStaysExtractor.extract(sources, Set.empty).cache()
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
    val followups : Dataset[Event[FollowUp]] = {
      //Extract Trackloss
      val tracklosses = {
        val tracklossConfig = TracklossesConfig(studyEnd = config.base.studyEnd)
        new Tracklosses(tracklossConfig).extract(sources).cache()
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

      val observations : Dataset[Event[ObservationPeriod]] = {
        val allEvents: Dataset[Event[AnyEvent]] = unionDatasets(
          drugPurchases.as[Event[AnyEvent]],
          diagnoses.as[Event[AnyEvent]]
        )
        val os : Dataset[Event[ObservationPeriod]] = new ObservationPeriodTransformer(
          config.observationPeriod.copy(events = Some(allEvents))).transform()
        os.cache()
        os
      }

      val patientsWithObservations = patients
        .joinWith(observations, patients.col("patientId") === observations.col("patientId"))


      val followup_config = config.followUp.copy(
        patients = Some(patientsWithObservations), dispensations = Some(drugPurchases),
        outcomes = Some(outcomes), tracklosses = Some(tracklosses)
      )
      val cachedFollowups = new FollowUpTransformer(followup_config)
        .transform().cache()

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
      val exposureConfig = ExposuresTransformerConfig(Some(patientsWithFollowups), Some(drugPurchases))
      new ExposuresTransformer(exposureConfig).transform()
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
