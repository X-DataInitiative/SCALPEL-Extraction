package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import fr.polytechnique.cmap.cnam.utilities.functions._

object ExposuresTransformer extends DatasetTransformer[FlatEvent, Exposure] {
  /**
    * todo: add scaladoc
    */
  final val followUpDelay = 180
  final val followUpEndMinInterval = 120
  final val exposureStartDelay = 90
  final val exposureStartMinInterval = 180
  final val startObservation = Timestamp.valueOf("2006-01-01 00:00:00")
  final val endObservation = Timestamp.valueOf("2009-12-31 23:59:59")

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    col("eventId").as("molecule"),
    col("exposureStart").as("start"),
    col("followUpEnd").as("end")
  )

  implicit class ExposuresDataFrame(data: DataFrame) {

    def addFollowUpStart: DataFrame = {
      val window = Window.partitionBy("patientID")

      val correctedStart = when(
        lower(col("category")) === "disease" ||
        lower(col("eventId")) === "benfluorex",
          lit(null)
      ).otherwise(col("start"))

      data
        .withColumn("followUpStart", date_add(min(correctedStart).over(window),
          followUpDelay).cast(TimestampType))
        .where(col("followUpStart") < endObservation)
    }

    def filterPatients: DataFrame = {
      val window = Window.partitionBy("patientID")

      // Drop patients whose got a target disease before the start of the follow up
      val diseaseFilter = min(
        when(
          col("category") === "disease" && col("start") < col("followUpStart"),
        lit(0)).otherwise((lit(1)))
      ).over(window).cast(BooleanType)

      // Drop patients whose first molecule event is after startObservation + 1 year
      val firstYearObservation = date_add(lit(startObservation), 365).cast(TimestampType)
      val drugFilter = max(
        when(
          col("category") === "molecule" && col("start") <= firstYearObservation,
          lit(1)).otherwise(lit(0))
      ).over(window).cast(BooleanType)

      data.withColumn("diseaseFilter", diseaseFilter)
        .withColumn("drugFilter", drugFilter)
        .where(col("diseaseFilter") && col("drugFilter"))
    }

    def computeExposureStart: DataFrame = {
      val window = Window.partitionBy("patientID", "eventId").orderBy("start")

      val exposureStartRule: Column = when(datediff(col("start"),
        col("previousStartDate")) <= exposureStartMinInterval,
        date_add(col("start"), exposureStartDelay).cast(TimestampType)
      ).otherwise(lit(null.asInstanceOf[Timestamp]))

      data
        .where(col("category") === "molecule")
        .withColumn("previousStartDate", lag(col("start"), 1).over(window))
        .withColumn("exposureStart", exposureStartRule)
        .withColumn("exposureStart", when(col("exposureStart") < col("followUpStart"),
          col("followUpStart")).otherwise(col("exposureStart"))
        )
        .where(col("exposureStart").isNotNull)
        .groupBy("patientID", "eventId")
        .agg(min(col("exposureStart")).as("exposureStart"))
    }

    def computeFollowUpEnd: DataFrame = {
      val window = Window.partitionBy("patientID")
      val firstTargetDisease = when(col("category") !== "disease", min(col("start")).over(window))

      data
        .withColumn("firstTargetDisease", firstTargetDisease)
        .withColumn("followUpEnd",
          minAmongColumns(col("deathDate"), col("firstTargetDisease"), lit(endObservation))
        )
    }
  }

  def transform(input: Dataset[FlatEvent]): Dataset[Exposure] = {
    import input.sqlContext.implicits._

    val events = input.toDF.repartition(col("patientID"))
    val eventsWithFollowUp = events.addFollowUpStart.filterPatients
    val dataWithExposureStart = eventsWithFollowUp.computeExposureStart
    val dataWithExposureEnd = eventsWithFollowUp.computeFollowUpEnd

    dataWithExposureStart
      .join(dataWithExposureEnd, Seq("patientID", "eventId"))
      .filter(col("start") < col("followUpEnd"))
      .select(outputColumns: _*)
      .as[Exposure]
  }
}
