package fr.polytechnique.cmap.cnam.etl.filters

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.events.{Event, Molecule, Outcome}
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
import fr.polytechnique.cmap.cnam.util.RichDataFrames._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object PatientFilters {

  // Drop patients who got the disease before the start of the follow up
  def filterEarlyDiagnosedPatients(data: Dataset[(Event[Outcome], FollowUp)], outcomeName: String): Dataset[(Event[Outcome], FollowUp)] = {

    val patientId = s"Event.${Event.Columns.PatientID}"
    val followUpStart = "FollowUp.followUpStart"
    val outcomeDate = s"Event.${Event.Columns.Start}"
    val value = s"Event.${Event.Columns.Value}"

    val window = Window.partitionBy(patientId)

    val diseaseFilter: Column = min(
      when(
          col(value) === outcomeName &&
          col(outcomeDate) < col(followUpStart), lit(0)
      ).otherwise(lit(1))
    ).over(window).cast(BooleanType)

    import data.sparkSession.implicits._
    renameTupleColumns(data)
      .withColumn("filter", diseaseFilter)
      .where(col("filter"))
      .drop("filter")
      .as[(Event[Outcome], FollowUp)]
  }

  // Drop patients whose first molecule event is after PeriodStart + 1 year
  def filterDelayedEntries(
      data: Dataset[Event[Molecule]],
      studyStart: Timestamp,
      delayedEntriesThreshold: Int = 12): Dataset[Event[Molecule]] = {

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

    import data.sparkSession.implicits._
    data
      .withColumn("filter", drugFilter)
      .where(col("filter"))
      .drop("filter")
      .as[Event[Molecule]]
  }
}
