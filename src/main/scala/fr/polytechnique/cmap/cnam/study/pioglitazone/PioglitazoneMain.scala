package fr.polytechnique.cmap.cnam.study.pioglitazone

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
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{ExposuresTransformer, ExposuresTransformerConfig}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.observation.ObservationPeriodTransformer
import fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes._
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions._


object PioglitazoneMain extends Main {

  val appName: String = "Pioglitazone"

  /**
    * Arguments expected:
    * "conf" -> "path/to/file.conf" (default: "$resources/config/pioglitazone/default.conf")
    * "env" -> "cnam" | "cmap" | "test" (default: "test")
    */
  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[_]] = {

    import sqlContext.implicits._
    import PatientFilters._

    val config = PioglitazoneConfig.load(argsMap("conf"), argsMap("env"))
    val inputPaths = config.input

    import implicits.SourceReader
    val sources = Sources.sanitizeDates(
      Sources.sanitize(sqlContext.readSources(config.input)),
      config.base.studyStart, config.base.studyEnd
    )

    val patients: Dataset[Patient] = new Patients(config.patients).extract(sources).cache()

    val drugPurchases: Dataset[Event[Molecule]] = new MoleculePurchases(config.molecules).extract(sources).cache()

    val diagnoses: Dataset[Event[Diagnosis]] = new Diagnoses(config.diagnoses).extract(sources).cache()

    val medicalActs = new MedicalActs(config.medicalActs).extract(sources)

    logger.info("Merging all events...")


    val outcomes = {
      val outcomesTransformer = new PioglitazoneOutcomeTransformer(config.outcomes.cancerDefinition)
      outcomesTransformer.transform(diagnoses, medicalActs).cache()
    }


    val followups = {

      val tracklosses = {
        val tracklossConfig = TracklossesConfig(studyEnd = config.base.studyEnd)
        new Tracklosses(tracklossConfig).extract(sources).cache()
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

      new FollowUpTransformer(config.followUp)
        .transform(patientsWithObservations, drugPurchases, outcomes, tracklosses)
        .cache()
    }

    val filteredPatients = {
      val firstFilterResult = if (config.filters.filterDelayedEntries) {
        patients.filterDelayedPatients(drugPurchases, config.base.studyStart, config.filters.delayedEntryThreshold)
      } else {
        patients
      }

      if (config.filters.filterDiagnosedPatients) {
        firstFilterResult.filterEarlyDiagnosedPatients(outcomes, followups, config.outcomes.cancerDefinition.toString)
      } else {
        patients
      }
    }

    val exposures = {
      val exposuresConfig = ExposuresTransformerConfig()
      val patientsWithFollowups = patients.joinWith(
        followups, followups.col("patientId") === patients.col("patientId")
      )
      new ExposuresTransformer(exposuresConfig).transform(patientsWithFollowups, drugPurchases)
    }

    Some(exposures)
  }
}