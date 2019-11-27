// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.observation

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, ObservationPeriod}
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

class ObservationPeriodTransformer(config: ObservationPeriodTransformerConfig) {

  import Columns._

  val outputColumns = List(
    col("patientID"),
    col("observationStart").as("start"),
    col("observationEnd").as("end")
  )

  def computeObservationStart(data: DataFrame): DataFrame = {
    val studyStart: Timestamp = config.studyStart
    val window = Window.partitionBy("patientID")
    val correctedStart = when(
      lower(col("category")) === "molecule" && (col("start") >= studyStart), col("start")
    )
    data.withColumn("observationStart", min(correctedStart).over(window).cast(TimestampType))
  }

  def computeObservationEnd(data: DataFrame): DataFrame = {
    val studyEnd: Timestamp = config.studyEnd
    data.withColumn("observationEnd", lit(studyEnd))
  }


  implicit class ObservationDataFrame(data: DataFrame) {
    def withObservationStart: DataFrame = computeObservationStart(data)

    def withObservationEnd: DataFrame = computeObservationEnd(data)
  }

  def transform(events: Dataset[Event[AnyEvent]]): Dataset[Event[ObservationPeriod]] = {

    import events.sqlContext.implicits._

    val observation = events.toDF
      .withObservationStart
      .withObservationEnd
      .dropDuplicates(Seq("patientID"))
      .map(
        ObservationPeriod.fromRow(
          _,
          patientIDCol = PatientID,
          startCol = ObservationStart,
          endCol = ObservationEnd
        )
      )

    observation.as
      [Event[ObservationPeriod]]
  }
}
