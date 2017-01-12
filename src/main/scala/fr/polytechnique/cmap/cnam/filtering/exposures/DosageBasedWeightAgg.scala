package fr.polytechnique.cmap.cnam.filtering.exposures

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DosageBasedWeightAgg(data: DataFrame) extends WeightAggregatorImpl(data) {

  def aggregateWeight(
      studyStart: Option[Timestamp],
      cumWindow: Option[Int],
      cumStartThreshold: Option[Int],
      cumEndThreshold: Option[Int]): DataFrame = {

    data.withColumn("weight", lit("dosage-based cumulative weight"))
  }
}
