package fr.polytechnique.cmap.cnam.featuring.cox

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.exposures.ExposuresConfig
import fr.polytechnique.cmap.cnam.etl.old_root.{DatasetTransformer, FilteringConfig, FlatEvent}
import fr.polytechnique.cmap.cnam.util.ColumnUtilities._

object CoxFollowUpEventsTransformer extends DatasetTransformer[FlatEvent, FlatEvent] {

  final lazy val followUpMonthsDelay: Int = ExposuresConfig.init().followUpDelay
  final lazy val diseaseCode: String = FilteringConfig.diseaseCode

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

  implicit class FollowUpFunctions(data: DataFrame) {

    // The following functions are supposed to be used on any dataset that contains follow-up events,
    //   in order to add  follow-up related columns, without changing the line count.
    // They are not used inside the FollowUpEventsTransformer object.
    def withFollowUpPeriodFromEvents: DataFrame = {
      val window = Window.partitionBy("patientID")

      val followUpStart: Column = when(col("category") === "followUpPeriod", col("start"))
      val followUpEnd: Column = when(col("category") === "followUpPeriod", col("end"))

      data
        .withColumn("followUpStart", min(followUpStart).over(window))
        .withColumn("followUpEnd", min(followUpEnd).over(window))
    }

    def withEndReasonFromEvents: DataFrame = {
      val window = Window.partitionBy("patientID")
      val endReason: Column = when(col("category") === "followUpPeriod", col("eventId"))
      data.withColumn("endReason", min(endReason).over(window))
    }
  }

  implicit class FollowUpDataFrame(data: DataFrame) {

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
        when(col("category") === "disease" && col("eventId") === "targetDisease", col("start"))
      ).over(window)

      data
        .withColumn("firstTargetDisease", firstTargetDisease)
        .withColumn("followUpEnd",
          minColumn(
            col("deathDate"),
            col("firstTargetDisease"),
            col("trackloss"),
            col("observationEnd")
          )
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
    import CoxObservationPeriodTransformer.ObservationFunctions
    import input.sqlContext.implicits._

    val events = input.toDF.repartition(col("patientID"))
    events
      .withObservationPeriodFromEvents
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
