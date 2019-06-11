package fr.polytechnique.cmap.cnam.etl.filters

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
import fr.polytechnique.cmap.cnam.util.RichDataFrame._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{Column, Dataset}

/*
 * The architectural decisions regarding the patient filters can be found in the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/109051905/Architecture+decisions
 */

private[filters] class PatientFiltersImplicits(patients: Dataset[Patient]) {

  import patients.sparkSession.implicits._

  private def applyContains(patientsToKeep: Set[String]) = {
    patients.filter {
      patient => patientsToKeep.contains(patient.patientID)
    }
  }

  private def applyRemoves(patientsToRemove: Set[String]) = {
    patients.filter {
      patient => !patientsToRemove.contains(patient.patientID)
    }
  }

  /** Converts a Dataset[Patient] to a local set. Useful for filtering patients from an event Dataset by using set.contains().
    * @return A local set containing the patients
    */
  def idsSet: Set[String] = patients.map(_.patientID).collect.toSet

  /** First, keep only patients with an Outcome and a Followup period.
    * Then, removes all patients who got a given Outcome event before the start of their follow-up period
    * @param outcomes A dataset of outcomes
    * @param followUpPeriods A dataset containing the follow-up periods of the patients
    * @param outcomeName The name of the outcome to find
    * @return a Dataset of patients with the unwanted patients removed
    */
  def filterEarlyDiagnosedPatients(
      outcomes: Dataset[Event[Outcome]],
      followUpPeriods: Dataset[Event[FollowUp]],
      outcomeName: String): Dataset[Patient] = {

    val patientId = s"_1.${Event.Columns.PatientID}"
    val followUpStart = "_2.start"
    val outcomeDate = s"_1.${Event.Columns.Start}"
    val value = s"_1.${Event.Columns.Value}"

    val joined = outcomes.joinWith(
      followUpPeriods, outcomes.col("patientID") === followUpPeriods.col("patientID")
    )

    val window = Window.partitionBy(patientId)

    val diseaseFilter: Column = min(
      when(
          col(value) === outcomeName &&
          col(outcomeDate) < col(followUpStart), lit(0)
      ).otherwise(lit(1))
    ).over(window).cast(BooleanType)

    val patientsToKeep: Set[String] = joined.withColumn("filter", diseaseFilter)
      .where(col("filter"))
      .select(patientId)
      .distinct
      .as[String]
      .collect()
      .toSet

    applyContains(patientsToKeep)
  }

  /** Removes all patients with an outcome event before the start of their follow-up period.
    * @param outcomes A dataset of outcomes
    * @param followUpPeriods A dataset containing the follow-up periods of the patients
    * @param outcomeName The name of the outcome to find
    * @return a Dataset of patients with the unwanted patients removed
    */
  def removeEarlyDiagnosedPatients(
    outcomes: Dataset[Event[Outcome]],
    followUpPeriods: Dataset[Event[FollowUp]],
    outcomeName: String): Dataset[Patient] = {

    val patientId = s"_1.${Event.Columns.PatientID}"
    val followUpStart = "_2.start"
    val outcomeDate = s"_1.${Event.Columns.Start}"
    val value = s"_1.${Event.Columns.Value}"

    val joined = outcomes.joinWith(
      followUpPeriods, outcomes.col("patientID") === followUpPeriods.col("patientID")
    )

    val window = Window.partitionBy(patientId)

    val diseaseFilter: Column = min(
      when(
        col(value) === outcomeName &&
          col(outcomeDate) < col(followUpStart), lit(0)
      ).otherwise(lit(1))
    ).over(window).cast(BooleanType)

    val patientsToRemove: Set[String] = joined.withColumn("filter", diseaseFilter)
      .where(!col("filter"))
      .select(patientId)
      .distinct
      .as[String]
      .collect()
      .toSet

    applyRemoves(patientsToRemove)
  }

  /** Removes all patients who haven't got a dispensation event within N months after the study start.
    *
    * @param dispensations The dispensation events to look at. It must contain only sub-types of dispensations
    * @param studyStart The date of start of the study
    * @param thresholdMonths The number of months of the initial period
    * @return a Dataset of patients with the unwanted patients removed
    */
  def filterDelayedPatients[Disp <: Dispensation](
      dispensations: Dataset[Event[Disp]],
      studyStart: Timestamp,
      thresholdMonths: Int = 12): Dataset[Patient] = {

    val window = Window.partitionBy(Event.Columns.PatientID)

    val firstYearObservation: Column = add_months(
      lit(studyStart),
      thresholdMonths
    ).cast(TimestampType)

    val drugFilter: Column = max(
      when(
        col(Event.Columns.Start) <= firstYearObservation,
        lit(1)
      ).otherwise(lit(0))
    ).over(window).cast(BooleanType)

    val patientsToKeep: Set[String] = dispensations
      .withColumn("filter", drugFilter)
      .where(col("filter"))
      .select("patientID")
      .distinct
      .as[String]
      .collect()
      .toSet

    applyContains(patientsToKeep)
  }

  /** Removes all patients who have got an event within N months after the study start.
    *
    * @param events The events to look at. Ideally, it should contain only molecule or drug events
    * @param studyStart The date of start of the study
    * @param thresholdMonths The number of months of the initial period
    * @return a Dataset of patients with the unwanted patients removed
    */
  def filterNoStartGap[T <: AnyEvent](
      events: Dataset[Event[T]],
      studyStart: Timestamp,
      thresholdMonths: Int = 2): Dataset[Patient] = {

    val window = Window.partitionBy(Event.Columns.PatientID)

    val gapLimitDate: Column = add_months(
      lit(studyStart),
      thresholdMonths
    ).cast(TimestampType)

    val drugFilter: Column = min(
      when(
        col(Event.Columns.Start) <= gapLimitDate,
        lit(0)
      ).otherwise(lit(1))
    ).over(window).cast(BooleanType)

    val patientsToKeep: Set[String] = events
        .withColumn("filter", drugFilter)
        .where(col("filter"))
        .select("patientID")
        .distinct
        .as[String]
        .collect()
        .toSet

    applyContains(patientsToKeep)
  }
}
