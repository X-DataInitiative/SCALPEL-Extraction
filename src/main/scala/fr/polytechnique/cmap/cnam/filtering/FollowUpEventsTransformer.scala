package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.utilities.ColumnUtilities._

object FollowUpEventsTransformer extends DatasetTransformer[FlatEvent, FlatEvent] {

  final val followUpMonthsDelay = 6
  final val diseaseCode = "C67"

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    lit("followUpPeriod").as("category"),
    col("endReason").as("eventId"),
    lit(1.0).as("weight"),
    col("followUpStart").as("start"),
    col("followUpEnd").as("end")
  )

  implicit class FollowUpDataFrame(data: DataFrame) {

    def withObservationPeriod: DataFrame = {
      val window = Window.partitionBy("patientID")

      val observationStart = when(lower(col("category")) === "observationperiod", col("start"))
      val observationEnd = when(lower(col("category")) === "observationperiod", col("end"))

      data
        .withColumn("observationStart", min(observationStart).over(window))
        .withColumn("observationEnd", min(observationEnd).over(window))
    }

    def withFollowUpStart: DataFrame = {
      val window = Window.partitionBy("patientID")

      val followUpStart = add_months(col("observationStart"), followUpMonthsDelay).cast(TimestampType)
      val correctedFollowUpStart = when(followUpStart < col("observationEnd"), followUpStart)

      data.withColumn("followUpStart", min(correctedFollowUpStart).over(window))
    }

    def withTrackloss: DataFrame = {
      val window = Window.partitionBy("patientID")

      val firstCorrectTrackloss = min(
        when(col("category") === "trackloss" && (col("start") > col("followUpStart")), col("start"))
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
          minColumn(col("deathDate"), col("firstTargetDisease"), col("trackloss"), col("observationEnd"))
        )
    }

    def withEndReason: DataFrame = {
      val endReason = when(
        col("followUpEnd") === col("deathDate"), "death"
      ).when(
        col("followUpEnd") === col("firstTargetDisease"), "disease"
      ).when(
        col("followUpEnd") === col("trackloss"), "trackloss"
      ).when(
        col("followUpEnd") === col("observationEnd"), "observationEnd"
      )

      data.withColumn("endReason", endReason)
    }
  }

  def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent] = {
    import input.sqlContext.implicits._

    val events = input.toDF.repartition(col("patientID"))
    events
      .withObservationPeriod
      .withFollowUpStart
      .withTrackloss
      .withFollowUpEnd
      .na.drop("any", Seq("followUpStart", "followUpEnd"))
      .withEndReason
      .select(outputColumns: _*)
      .dropDuplicates(Seq("PatientID"))
      .as[FlatEvent]
  }
}
