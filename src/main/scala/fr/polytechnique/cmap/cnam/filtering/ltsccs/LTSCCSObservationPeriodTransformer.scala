package fr.polytechnique.cmap.cnam.filtering.ltsccs

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.filtering.{DatasetTransformer, FlatEvent}

// For the LTSCCS Model, we calculate the observation periods based on the exposures.
// The observation start of a patient is defined of the first exposure start and the observation end
//   is defined as the last exposure end.
object LTSCCSObservationPeriodTransformer extends DatasetTransformer[FlatEvent, FlatEvent] {

  val groupCols = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate")
  )

  val outputCols = groupCols ++ List(
    lit("observationPeriod").as("category"),
    lit("observationPeriod").as("eventId"),
    lit(1.0).as("weight"),
    col("observationStart").as("start"),
    col("observationEnd").as("end")
  )

  override def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent] = {
    import input.sqlContext.implicits._
    val exposures = input.toDF.where(col("category") === "exposure")

    exposures
      .groupBy(groupCols: _*)
      .agg(
        min(col("start")).as("observationStart"),
        max(col("end")).as("observationEnd")
      )
      .select(outputCols: _*)
      .as[FlatEvent]
  }
}
