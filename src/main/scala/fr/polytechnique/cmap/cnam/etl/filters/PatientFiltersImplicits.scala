package fr.polytechnique.cmap.cnam.etl.filters

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.events.{Event, Molecule, Outcome}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
import fr.polytechnique.cmap.cnam.util.RichDataFrames._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{Column, Dataset}

private[filters] class PatientFiltersImplicits(patients: Dataset[Patient]) {

  // Drop patients who got an outcome before the start of the follow up
  def filterEarlyDiagnosedPatients(
      outcomes: Dataset[Event[Outcome]],
      followUpPeriods: Dataset[FollowUp],
      outcomeName: String): Dataset[Patient] = {

    val joined = outcomes.joinWith(followUpPeriods, outcomes.col("patientID") === followUpPeriods.col("patientID"))
    val patientId = s"Event.${Event.Columns.PatientID}"
    val followUpStart = "FollowUp.start"
    val outcomeDate = s"Event.${Event.Columns.Start}"
    val value = s"Event.${Event.Columns.Value}"

    val window = Window.partitionBy(patientId)

    val diseaseFilter: Column = min(
      when(
          col(value) === outcomeName &&
          col(outcomeDate) < col(followUpStart), lit(0)
      ).otherwise(lit(1))
    ).over(window).cast(BooleanType)

    val patientsToKeep = renameTupleColumns(joined)
      .withColumn("filter", diseaseFilter)
      .where(col("filter"))
      .select(patientId)
      .distinct

    import patients.sparkSession.implicits._
    patients.join(patientsToKeep, "patientID").as[Patient]
  }

  // Drop patients whose first molecule event is after PeriodStart + 1 year
  def filterDelayedEntries(
      molecules: Dataset[Event[Molecule]],
      studyStart: Timestamp,
      delayedEntriesThreshold: Int = 12): Dataset[Patient] = {

    val window = Window.partitionBy(Event.Columns.PatientID)

    val firstYearObservation = add_months(
      lit(studyStart),
      delayedEntriesThreshold
    ).cast(TimestampType)

    val drugFilter = max(
      when(
        col(Event.Columns.Start) <= firstYearObservation,
        lit(1)
      ).otherwise(lit(0))
    ).over(window).cast(BooleanType)

    val patientsToKeep = molecules
      .withColumn("filter", drugFilter)
      .where(col("filter"))
      .drop("filter")
      .select("patientID")
      .distinct

    import patients.sparkSession.implicits._
    patients.join(patientsToKeep, "patientID").as[Patient]
  }
}
