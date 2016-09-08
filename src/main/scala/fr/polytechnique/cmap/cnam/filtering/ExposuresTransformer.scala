package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.utilities.ColumnUtilities._

object ExposuresTransformer extends DatasetTransformer[FlatEvent, FlatEvent] {

  // Constant definitions for delays and time windows. Should be verified before compiling.
  // In the future, we may want to export them to an external file.
  final val followUpDelay = 180
  final val followUpEndMinInterval = 120
  final val exposureStartDelay = 90
  final val exposureStartMinInterval = 180
  final val startObservation = Timestamp.valueOf("2006-01-01 00:00:00")
  final val endObservation = Timestamp.valueOf("2009-12-31 23:59:59")
  final val diseaseCode = "C67"

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    lit("exposure").as("category"),
    col("eventId"),
    lit(1.0).as("weight"),
    col("exposureStart").as("start"),
    col("followUpEnd").as("end")
  )

  implicit class ExposuresDataFrame(data: DataFrame) {

    def withFollowUpStart: DataFrame = {
      val window = Window.partitionBy("patientID")

      val correctedStart = when(lower(col("category")) === "molecule", col("start")) // NULL otherwise

      data
        .withColumn("followUpStart",
          date_add(min(correctedStart).over(window), followUpDelay).cast(TimestampType)
        )
        .where(col("followUpStart") < endObservation)
    }

    def filterPatients: DataFrame = {
      val window = Window.partitionBy("patientID")

      // Drop patients whose got a target disease before the start of the follow up
      val diseaseFilter = min(
        when(
          col("category") === "disease" && (col("start") < col("followUpStart")),
        lit(0)).otherwise(lit(1))
      ).over(window).cast(BooleanType)

      // Drop patients whose first molecule event is after startObservation + 1 year
      val firstYearObservation = date_add(lit(startObservation), 365).cast(TimestampType)
      val drugFilter = max(
        when(
          col("category") === "molecule" && (col("start") <= firstYearObservation),
          lit(1)
        ).otherwise(lit(0))
      ).over(window).cast(BooleanType)

      // Drop patients who only got tracklosses event
      val tracklossFilter = max(
        when(col("category") === "trackloss",
          lit(0)
        ).otherwise(lit(1))
      ).over(window).cast(BooleanType)

      data.withColumn("diseaseFilter", diseaseFilter)
        .withColumn("drugFilter", drugFilter)
        .withColumn("tracklossFilter", tracklossFilter)
        .where(col("diseaseFilter") && col("drugFilter") && col("tracklossFilter"))
    }

    def withTrackloss: DataFrame = {
      val window = Window.partitionBy("patientID")

      val firstCorrectTrackloss = min(
        when(
        col("category") === "trackloss" &&
        col("start") > col("followUpStart"),
        col("start"))
      ).over(window)

      data.withColumn("trackloss", firstCorrectTrackloss)
    }

    def withFollowUpEnd: DataFrame = {
      val window = Window.partitionBy("patientID")

      val firstTargetDisease = min(
        when(col("category") === "disease" && col("eventId") === diseaseCode, col("start"))
      ).over(window)

      data
        .withColumn("firstTargetDisease", firstTargetDisease)
        .withColumn("followUpEnd",
          minColumn(col("deathDate"), col("firstTargetDisease"), col("trackloss"), lit(endObservation))
        )
    }

    // Ideally, this method must receive only molecules events, otherwise they will treat diseases
    //   as molecules and add an exposure start date for them.
    // The exposure start date will be null when the patient was not exposed.
    def withExposureStart: DataFrame = {
      val window = Window.partitionBy("patientID", "eventId")

      val exposureStartRule: Column = when(
        datediff(col("start"), col("previousStartDate")) <= exposureStartMinInterval,
          date_add(col("start"), exposureStartDelay).cast(TimestampType)
      )

      data
        .withColumn("previousStartDate", lag(col("start"), 1).over(window.orderBy("start")))
        .withColumn("exposureStart", exposureStartRule)
        .withColumn("exposureStart", when(col("exposureStart") < col("followUpStart"),
          col("followUpStart")).otherwise(col("exposureStart"))
        )
        .withColumn("exposureStart", min("exposureStart").over(window))
    }
  }

  def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent] = {
    import input.sqlContext.implicits._

    val events = input.toDF.repartition(col("patientID"))
    events
      .withFollowUpStart
      .filterPatients
      .withTrackloss
      .withFollowUpEnd
      .where(col("start") < col("followUpEnd"))
      .where(col("category") === "molecule")
      .withExposureStart
      .where(col("exposureStart").isNotNull)
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID", "eventId"))
      .as[FlatEvent]
  }
}
