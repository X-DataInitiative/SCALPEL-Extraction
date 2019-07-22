package fr.polytechnique.cmap.cnam.etl.transformers.observation

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.transformers._
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

class ObservationPeriodTransformer (config: ObservationPeriodTransformerConfig) extends Serializable {

  val outputColumns = List(
    col("patientID"),
    col("observationStart").as("start"),
    col("observationEnd").as("stop")
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

  def transform(): Dataset[Event[ObservationPeriod]] = {
    val events = config.events.get
    import events.sqlContext.implicits._
    events.toDF
      .withObservationStart
      .withObservationEnd
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID"))
      .map(ObservationPeriod.fromRow(_))
  }
}
