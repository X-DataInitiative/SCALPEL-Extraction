package fr.polytechnique.cmap.cnam.etl.transformer.observation

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}


class ObservationPeriodTransformer (
  val studyStart: Timestamp,
  val studyEnd: Timestamp) {

  val outputColumns = List(
    col("patientID"),
    col("observationStart").as("start"),
    col("observationEnd").as("stop")
  )

  def computeObservationStart(data: DataFrame): DataFrame =  {
    val window = Window.partitionBy("patientID")
    val correctedStart = when(
      lower(col("category")) === "molecule" && (col("start") >= studyStart), col("start")
    )
    data.withColumn("observationStart", min(correctedStart).over(window).cast(TimestampType))
  }

  def computeObservationEnd(data: DataFrame): DataFrame = {
    data.withColumn("observationEnd", lit(studyEnd))
  }


  implicit class ObservationDataFrame(data: DataFrame) {
    def withObservationStart: DataFrame = computeObservationStart(data)
    def withObservationEnd: DataFrame = computeObservationEnd(data)
  }

  def transform(events: Dataset[Event[AnyEvent]]): Dataset[ObservationPeriod] = {

    import events.sqlContext.implicits._

    events.toDF
      .withObservationStart
      .withObservationEnd
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID"))
      .as[ObservationPeriod]
  }
}
