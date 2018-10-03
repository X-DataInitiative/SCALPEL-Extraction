package fr.polytechnique.cmap.cnam.study.pioglitazone

import java.io.PrintWriter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActs
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.Diagnoses
import fr.polytechnique.cmap.cnam.etl.extractors.molecules.MoleculePurchases
import fr.polytechnique.cmap.cnam.etl.extractors.patients.Patients
import fr.polytechnique.cmap.cnam.etl.extractors.tracklosses.{Tracklosses, TracklossesConfig}
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.ExposuresTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformer
import fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes._
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions._
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}


object PioglitazoneMain extends Main {

  val appName: String = "Pioglitazone"

  /**
    * Arguments expected:
    * "conf" -> "path/to/file.conf" (default: "$resources/config/pioglitazone/default.conf")
    * "env" -> "cnam" | "cmap" | "test" (default: "test")
    */
  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[_]] = {

    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()
    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    import sqlContext.implicits._
    import PatientFilters._

    val config = PioglitazoneConfig.load(argsMap("conf"), argsMap("env"))

    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(config.input))

    val patients: Dataset[Patient] = new Patients(config.patients).extract(sources).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "extract_patients",
          List("DCIR", "MCO", "IR_BEN_R"),
          OperationTypes.Patients,
          patients.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

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

    val medicalActs = new MedicalActs(config.medicalActs).extract(sources).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "acts",
          List("DCIR", "MCO", "MCO_CE"),
          OperationTypes.MedicalActs,
          medicalActs.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

    val outcomes = {
      val outcomesTransformer = new PioglitazoneOutcomeTransformer(config.outcomes.cancerDefinition)
      outcomesTransformer.transform(diagnoses, medicalActs).cache()
    }
    operationsMetadata += {
      OperationReporter
        .report(
          "outcomes",
          List("acts", "diagnoses"),
          OperationTypes.Outcomes,
          outcomes.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }


    val followups = {
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

      val observations = {
        // TODO: Observations only need and uses drugPurchases.
        val allEvents: Dataset[Event[AnyEvent]] = unionDatasets(
          drugPurchases.as[Event[AnyEvent]],
          diagnoses.as[Event[AnyEvent]]
        )
        new ObservationPeriodTransformer(config.observationPeriod).transform(allEvents).cache()
      }

      val patientsWithObservations = patients
        .joinWith(observations, patients.col("patientId") === observations.col("patientId"))

      new FollowUpTransformer(config.followUp)
        .transform(patientsWithObservations, drugPurchases, outcomes, tracklosses)
        .cache()
    }

    operationsMetadata += {
      OperationReporter
        .report(
          "followup",
          List("drug_purchases", "outcomes", "trackloss"),
          OperationTypes.AnyEvents,
          followups.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

    val filteredPatientsAncestors = new ListBuffer[String]
    val cnamPaperBaseCohort = {
      val firstFilterResult = if (config.filters.filterDelayedEntries) {
        filteredPatientsAncestors += "drug_purchases"
        val delayedFreePatients = patients
          .filterDelayedPatients(drugPurchases, config.base.studyStart, config.filters.delayedEntryThreshold).cache()

        operationsMetadata += {
          OperationReporter
            .report(
              "early_diabetics",
              List("drug_purchases"),
              OperationTypes.Patients,
              delayedFreePatients.toDF,
              Path(config.output.outputSavePath),
              config.output.saveMode
            )
        }
        delayedFreePatients
      } else {
        patients
      }

      val secondFilterResult = if (config.filters.filterDiagnosedPatients) {

        filteredPatientsAncestors ++= List("outcomes", "followup")
        val earlyDiagnosedPatients = firstFilterResult
          .removeEarlyDiagnosedPatients(outcomes, followups, config.outcomes.cancerDefinition.toString).cache()

        operationsMetadata += {
          OperationReporter
            .report(
              "early_diagnosed_with_follow_up",
              filteredPatientsAncestors.toList,
              OperationTypes.Patients,
              earlyDiagnosedPatients.toDF,
              Path(config.output.root)
            )
        }
        earlyDiagnosedPatients

      } else {
        firstFilterResult
      }

      import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformer.FollowUpDataset
      val cleanFollowUps = followups.cleanFollowUps()
      filteredPatientsAncestors += "clean_follow_up"
      secondFilterResult.joinWith(
        cleanFollowUps, cleanFollowUps.col("patientId") === patients.col("patientId")
      )
    }

    operationsMetadata += {
      OperationReporter
        .report(
          "cnam_paper_cohort",
          filteredPatientsAncestors.toList,
          OperationTypes.Patients,
          cnamPaperBaseCohort.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

    val exposures = new ExposuresTransformer(config.exposures).transform(cnamPaperBaseCohort, drugPurchases)
    operationsMetadata += {
      OperationReporter
        .report(
          "exposures",
          List("cnam_paper_cohort", "drug_purchases", "followup"),
          OperationTypes.Exposures,
          exposures.toDF,
          Path(config.output.outputSavePath),
          config.output.saveMode
        )
    }

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata_pioglitazone_" + format.format(startTimestamp) + ".json") {
      write(metadataJson)
      close()
    }

    Some(exposures)
  }
}