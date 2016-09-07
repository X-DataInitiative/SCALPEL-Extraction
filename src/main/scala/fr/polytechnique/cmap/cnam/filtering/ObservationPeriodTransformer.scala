package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset}

object ObservationPeriodTransformer extends DatasetTransformer[FlatEvent, FlatEvent] {

  final val ObservationEnd = Timestamp.valueOf("2009-12-31 23:59:59")

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    lit("observationPeriod").as("category"),
    lit("observationPeriod").as("eventId"),
    lit(1.0).as("weight"),
    col("observationStart").as("start"),
    lit(ObservationEnd).as("end")
  )

  implicit class ObservationDataFrame(data: DataFrame) {
    def withObservationStart: DataFrame = {
      val window = Window.partitionBy("patientID")
      val correctedStart = when(lower(col("category")) === "molecule", col("start")) // NULL otherwise
      data.withColumn("observationStart",min(correctedStart).over(window).cast(TimestampType))
    }
  }

  def transform(events: Dataset[FlatEvent]): Dataset[FlatEvent] = {
    import events.sqlContext.implicits._

    events.toDF
      .withObservationStart
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID"))
      .as[FlatEvent]
  }
}
