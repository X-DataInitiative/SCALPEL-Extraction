package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.events.Trackloss
import fr.polytechnique.cmap.cnam.util.ColumnUtilities._
import fr.polytechnique.cmap.cnam.util.functions.daysBetween

class MLPPDataFrameImplicits(data: DataFrame) {

  import data.sparkSession.implicits._

  def withAge(minTimestamp: Timestamp): DataFrame = {
    val age: Column = year(lit(minTimestamp)) - year(col("birthDate"))
    data.withColumn("age", age)
  }

  def withStartBucket(minTimestamp: Timestamp, maxTimestamp: Timestamp, bucketSize: Int): DataFrame = {
    data.withColumn("startBucket",
      col("start").bucketize(minTimestamp, maxTimestamp, bucketSize)
    )
  }

  def withDeathBucket(minTimestamp: Timestamp, maxTimestamp: Timestamp, bucketSize: Int): DataFrame = {
    data.withColumn("deathBucket",
      col("deathDate").bucketize(minTimestamp, maxTimestamp, bucketSize)
    )
  }

  def withTracklossBucket(bucketCount: Int): DataFrame = {
    val window = Window.partitionBy("patientId")

    val hadTrackloss: Column = (col("category") === Trackloss.category) &&
      (col("startBucket") < minColumn(col("deathBucket"), lit(bucketCount)))

    val tracklossBucket: Column = min(when(hadTrackloss, col("startBucket"))).over(window)

    data.withColumn("tracklossBucket", tracklossBucket)
  }

  // TODO : This one should also disappear since we compute the end date in the Exposure transformer
  // We are no longer using disease information for calculating the end bucket.
  def withEndBucket(includeCensoredBucket: Boolean, bucketCount: Int): DataFrame = {

    val deathBucketRule = if (includeCensoredBucket) col("deathBucket") + 1 else col("deathBucket")
    val tracklossBucketRule = if (includeCensoredBucket) col("tracklossBucket") + 1 else col("tracklossBucket")

    val endBucket: Column = minColumn(deathBucketRule, tracklossBucketRule, lit(bucketCount))
    data.withColumn("endBucket", endBucket)
  }

  def withIndices(columnNames: Seq[String]): DataFrame = {

    columnNames.foldLeft(data) {
      (currentData, columnName) => {

        // WARNING: The following code is dangerous, but there is no perfect solution.
        //   It was tested with up to 30 million distinct values with 15 characters and 300
        //   million total rows. If we have more than that, we may need to fall back to a join
        //   option, which would take very much longer.
        val labels = currentData
          .select(col(columnName).cast(StringType))
          .distinct
          .map(_.getString(0))
          .collect
          .toSeq
          .sorted

        // It's transient to avoid serializing the full Map. Only the labels Seq will be
        // serialized and each executor will compute their own Map. This is slower but allows more
        // labels to be indexed.
        @transient lazy val indexerFuc = labels.zipWithIndex.toMap.apply _

        val indexer = udf(indexerFuc)

        currentData.withColumn(columnName + "Index", indexer(col(columnName)))
      }
    }
  }
}

class MLPPDataFrame(params: MLPPLoader.Params) {

  private val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt

  implicit def toMLPPDataFrame(dataFrame: DataFrame): MLPPDataFrameImplicits = new MLPPDataFrameImplicits(dataFrame)

  def makeMLPPDataFrame(input: DataFrame): DataFrame = {
    input
      .withAge(params.minTimestamp)
      .withStartBucket(params.minTimestamp, params.maxTimestamp, params.bucketSize)
      .withDeathBucket(params.minTimestamp, params.maxTimestamp, params.bucketSize)
      .withTracklossBucket(bucketCount)
      .withEndBucket(params.includeCensoredBucket, bucketCount)
  }
}
