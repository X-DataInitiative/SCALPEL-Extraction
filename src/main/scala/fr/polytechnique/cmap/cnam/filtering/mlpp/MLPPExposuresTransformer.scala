package fr.polytechnique.cmap.cnam.filtering.mlpp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.{ExposuresTransformer, FlatEvent}

object MLPPExposuresTransformer extends ExposuresTransformer {

  private def minPurchases = MLPPConfig.exposureDefinition.minPurchases
  private def startDelay = MLPPConfig.exposureDefinition.startDelay
  private def purchasesWindow = MLPPConfig.exposureDefinition.purchasesWindow
  private def onlyFirstExposure =  MLPPConfig.exposureDefinition.onlyFirst
  private def filterLostPatients = MLPPConfig.exposureDefinition.filterLostPatients
  private def filterDelayedEntries  = MLPPConfig.exposureDefinition.filterDelayedEntries
  private def delayedEntryThreshold = MLPPConfig.exposureDefinition.delayedEntryThreshold
  private def filterEarlyDiagnosedPatients = MLPPConfig.exposureDefinition.filterEarlyDiagnosedPatients
  private def diagnosedPatientsThreshold = MLPPConfig.exposureDefinition.diagnosedPatientsThreshold
  private def filterNeverSickPatients = MLPPConfig.exposureDefinition.filterNeverSickPatients

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    lit("exposure").as("category"),
    col("eventId"),
    lit(1.0).as("weight"),
    col("exposureStart").as("start"),
    col("exposureEnd").as("end")
  )

  implicit class ExposuresDataFrame(data: DataFrame) {

    /**
      * Drops patients who got a target disease before before a threshold (measured by a number of
      *   months after study start, parameterizable with `diagnosedPatientsThreshold`)
      */
    def filterEarlyDiagnosedPatients(doFilter: Boolean): DataFrame = {

      if (doFilter) {
        val window = Window.partitionBy("patientID")

        val dateThreshold: Column = add_months(
          lit(StudyStart), diagnosedPatientsThreshold
        ).cast(TimestampType)

        val filterColumn: Column = min(
          when(
            (col("category") === "disease") &&
            (col("eventId") === "targetDisease") &&
            (col("start") < dateThreshold), lit(0)
          ).otherwise(lit(1))
        ).over(window).cast(BooleanType)

        data.withColumn("filter", filterColumn).where(col("filter")).drop("filter")
      }
      else {
        data
      }
    }

    /**
      * Drops patients whose first molecule event is after StudyStart + delay (default: 1 year)
      */
    def filterDelayedEntries(doFilter: Boolean): DataFrame = {
      if (doFilter) {
        val window = Window.partitionBy("patientID")

        val firstYearObservation: Column = add_months(
          lit(StudyStart), delayedEntryThreshold
        ).cast(TimestampType)

        val filterColumn: Column = max(
          when(
            col("category") === "molecule" && (col("start") <= firstYearObservation),
            lit(1)
          ).otherwise(lit(0))
        ).over(window).cast(BooleanType)

        data.withColumn("filter", filterColumn).where(col("filter")).drop("filter")
      }
      else {
        data
      }
    }

    /**
      * Drops patients with trackloss events
      */
    def filterLostPatients(doFilter: Boolean): DataFrame = {
      if (doFilter) {
        val window = Window.partitionBy("patientID")
        val filterColumn: Column = min(
          when(
            col("category") === "trackloss" && (col("start") >= StudyStart),
            lit(0)
          ).otherwise(lit(1))
        ).over(window).cast(BooleanType)

        data.withColumn("filter", filterColumn).where(col("filter")).drop("filter")
      }
      else {
        data
      }
    }

    /**
      * Drops patients who never had a target disease event
      */
    def filterNeverSickPatients(doFilter: Boolean): DataFrame = {

      if (doFilter) {
        val window = Window.partitionBy("patientID")

        val filterColumn: Column = max(
          when(
            (col("category") === "disease") &&
            (col("eventId") === "targetDisease") &&
            (col("start") <= MLPPConfig.maxTimestamp), lit(1)
          ).otherwise(lit(0))
        ).over(window).cast(BooleanType)

        data.withColumn("filter", filterColumn).where(col("filter")).drop("filter")
      }
      else {
        data
      }
    }

    def withExposureStart(minPurchases: Int = 1, intervalSize: Int = 6,
        startDelay: Int = 0, firstOnly: Boolean = false): DataFrame = {

      val window = Window.partitionBy("patientID", "eventId")

      // We don't lag the column if we want one exposure for every purchase
      val laggedStart: Column = if(minPurchases == 1)
        col("start")
      else
        lag(col("start"), minPurchases - 1).over(window.orderBy("start"))

      val exposureStartRule: Column = when(
        months_between(col("start"), col("previousStartDate")) <= intervalSize,
        add_months(col("start"), startDelay).cast(TimestampType)
      )

      val finalExposureStart: Column = if(firstOnly)
        min(exposureStartRule).over(window)
      else
        exposureStartRule

      data
        .withColumn("previousStartDate", laggedStart)
        .withColumn("exposureStart", finalExposureStart)
    }

    // For now, exposure end is the same as exposure start
    def withExposureEnd: DataFrame = {
      data.withColumn("exposureEnd", col("exposureStart"))
    }
  }

  def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent] = {

    import input.sqlContext.implicits._

    input.toDF
      .filterDelayedEntries(filterDelayedEntries)
      .filterEarlyDiagnosedPatients(filterEarlyDiagnosedPatients)
      .filterLostPatients(filterLostPatients)
      .filterNeverSickPatients(filterNeverSickPatients)
      .where(col("category") === "molecule")
      .withExposureStart(
        minPurchases = minPurchases,
        intervalSize = purchasesWindow,
        startDelay = startDelay,
        firstOnly = onlyFirstExposure
      )
      .withExposureEnd
      .where(col("exposureStart").isNotNull)
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID", "eventId", "start", "end"))
      .as[FlatEvent]
  }
}
