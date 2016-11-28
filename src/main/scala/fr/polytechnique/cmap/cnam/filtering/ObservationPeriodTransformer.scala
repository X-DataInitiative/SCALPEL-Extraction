package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

trait ObservationPeriodTransformer extends DatasetTransformer[FlatEvent, FlatEvent]{

  final val StudyStart: Timestamp = FilteringConfig.dates.studyStart
  final val StudyEnd: Timestamp = FilteringConfig.dates.studyEnd

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    lit("observationPeriod").as("category"),
    lit("observationPeriod").as("eventId"),
    lit(1.0).as("weight"),
    col("observationStart").as("start"),
    col("observationEnd").as("end")
  )

  def computeObservationStart(data: DataFrame): DataFrame
  def computeObservationEnd(data: DataFrame): DataFrame

  implicit class ObservationFunctions(data: DataFrame) {

    // The following function is supposed to be used on any dataset that contains observationPeriod
    //   events, in order to add the observation start and end as columns, without changing the line
    //   count.
    // It is not used inside the ObservationPeriodTransformer object.
    def withObservationPeriodFromEvents: DataFrame = {
      val window = Window.partitionBy("patientID")

      val observationStart = when(lower(col("category")) === "observationperiod", col("start"))
      val observationEnd = when(lower(col("category")) === "observationperiod", col("end"))

      data
        .withColumn("observationStart", min(observationStart).over(window))
        .withColumn("observationEnd", min(observationEnd).over(window))
    }
  }

  implicit class ObservationDataFrame(data: DataFrame) {
    def withObservationStart: DataFrame = computeObservationStart(data)
    def withObservationEnd: DataFrame = computeObservationEnd(data)
  }

  def transform(events: Dataset[FlatEvent]): Dataset[FlatEvent] = {
    import events.sqlContext.implicits._

    events.toDF
      .withObservationStart
      .withObservationEnd
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID"))
      .as[FlatEvent]
  }
}
