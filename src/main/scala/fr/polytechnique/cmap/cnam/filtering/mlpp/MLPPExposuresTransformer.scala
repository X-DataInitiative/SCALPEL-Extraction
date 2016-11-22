package fr.polytechnique.cmap.cnam.filtering.mlpp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.{ExposuresTransformer, FlatEvent}

object MLPPExposuresTransformer extends ExposuresTransformer {

  final val ExposureMinPurchases = 1
  final val ExposureStartDelay = 0
  final val ExposureStartThreshold = 6
  final val OnlyFirstExposure = false
  final val FilterDelayedEntries = true
  final val FilterDiagnosedPatients = true

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
      * Drops patients whose got a target disease before periodStart
      */
    def filterDiagnosedPatients(doFilter: Boolean): DataFrame = doFilter match {
      case false => data
      case true => {
        val window = Window.partitionBy("patientID")

        val filterColumn: Column = min(
          when(
            col("category") === "disease" &&
              col("eventId") === "targetDisease" &&
              (col("start") < StudyStart), lit(0)
          ).otherwise(lit(1))
        ).over(window).cast(BooleanType)

        data.withColumn("filter", filterColumn).where(col("filter")).drop("filter")
      }
    }

    /**
      * Drops patients whose first molecule event is after StudyStart + 1 year
      */
    def filterDelayedEntries(doFilter: Boolean): DataFrame = doFilter match {
      case false => data
      case true => {
        val window = Window.partitionBy("patientID")

        val firstYearObservation: Column = add_months(lit(StudyStart), 12).cast(TimestampType)
        val filterColumn: Column = max(
          when(
            col("category") === "molecule" && (col("start") <= firstYearObservation),
            lit(1)
          ).otherwise(lit(0))
        ).over(window).cast(BooleanType)

        data.withColumn("filter", filterColumn).where(col("filter")).drop("filter")
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
      .filterDelayedEntries(FilterDelayedEntries)
      .filterDiagnosedPatients(FilterDiagnosedPatients)
      .where(col("category") === "molecule")
      .withExposureStart(
        minPurchases = ExposureMinPurchases,
        intervalSize = ExposureStartThreshold,
        startDelay = ExposureStartDelay,
        firstOnly = OnlyFirstExposure
      )
      .withExposureEnd
      .where(col("exposureStart").isNotNull)
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID", "eventId", "start", "end"))
      .as[FlatEvent]
  }
}
