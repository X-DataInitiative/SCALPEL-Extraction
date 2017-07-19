package fr.polytechnique.cmap.cnam.etl.old_root.exposures

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}

class PatientFiltersImplicits(data: DataFrame) {

  // Drop patients who got the disease before the start of the follow up
  def filterEarlyDiagnosedPatients(doFilter: Boolean, diseaseCode: String): DataFrame = {

    if (doFilter) {
      val window = Window.partitionBy("patientID")

      val diseaseFilter: Column = min(
        when(
          col("category") === "disease" &&
            col("eventId") === diseaseCode &&
            col("start") < col("followUpStart"), lit(0))
          .otherwise(lit(1))
      ).over(window).cast(BooleanType)

      data.withColumn("filter", diseaseFilter).where(col("filter")).drop("filter")
    }
    else data
  }

  // Drop patients whose first molecule event is after PeriodStart + 1 year
  def filterDelayedEntries(doFilter: Boolean, studyStart: Timestamp,
    delayedEntriesThreshold: Int = 12): DataFrame = {

    if (doFilter) {
      val window = Window.partitionBy("patientID")

      val firstYearObservation = add_months(
        lit(studyStart),
        delayedEntriesThreshold
      ).cast(TimestampType)

      val drugFilter = max(
        when(
          col("category") === "molecule" && (col("start") <= firstYearObservation),
          lit(1)
        ).otherwise(lit(0))
      ).over(window).cast(BooleanType)

      data.withColumn("filter", drugFilter).where(col("filter")).drop("filter")
    }
    else data
  }

  def filterPatients(
    studyStart: Timestamp,
    diseaseCode: String,
    delayedEntries: Boolean = true,
    delayedEntriesThreshold: Int = 12,
    earlyDiagnosed: Boolean = true,
    lostPatients: Boolean = false,
    neverSickPatients: Boolean = false): DataFrame = {

    filterEarlyDiagnosedPatients(earlyDiagnosed, diseaseCode)
    filterDelayedEntries(delayedEntries, studyStart, delayedEntriesThreshold)
  }
}
