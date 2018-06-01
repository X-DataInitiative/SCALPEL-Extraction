package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, min, when}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.events.Outcome
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.minColumn
import fr.polytechnique.cmap.cnam.util.RichDataFrame._
import fr.polytechnique.cmap.cnam.util.functions.daysBetween


class MLPPOutcomesImplicits(dataFrame: DataFrame) {

  def withDiseaseType: DataFrame = {
    dataFrame.withColumnRenamed("groupID", "diseaseType")
  }

  def withDiseaseBucket(keepFirstOnly: Boolean, bucketCount: Int): DataFrame = {
    val hadDisease: Column = (col("category") === Outcome.category) &&
      (col("startBucket") < minColumn(col("endBucket"), lit(bucketCount)))

    val diseaseBucket: Column =
      if (keepFirstOnly) {
        val window = Window.partitionBy("patientID", "diseaseType")
        min(when(hadDisease, col("startBucket"))).over(window)
      }
      else
        when(hadDisease, col("startBucket"))

    dataFrame.withColumn("diseaseBucket", diseaseBucket)
  }

  def makeOutcomes(featuresAsList: Boolean, bucketCount: Int): DataFrame = {
    if (featuresAsList) {
      dataFrame
        .select(Seq(col("patientIDIndex"), col("diseaseBucket"), col("diseaseTypeIndex")): _*)
        .distinct.toDF("patientIndex", "bucket", "diseaseType")
    }
    else {
      dataFrame
        .withColumn("patientBucketIndex", col("patientIDIndex") * bucketCount + col("diseaseBucket"))
        .select(Seq(col("patientBucketIndex"), col("diseaseTypeIndex")): _*)
        .distinct
    }
  }

  // The following function assumes the data has been filtered and contains only patients with the disease
  def makeStaticOutcomes: DataFrame = {
    dataFrame.select("patientIDIndex").distinct.toDF
  }

  def writeLookupFiles(rootDir: String): Unit = {
    dataFrame
      .select("diseaseType", "diseaseTypeIndex")
      .dropDuplicates(Seq("diseaseType"))
      .writeCSV(s"$rootDir/csv/OutcomesLookup.csv")
  }
}

class MLPPOutcomes(params: MLPPLoader.Params) {

  private val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt

  implicit def toMLPPOutcomesImplicits(dataFrame: DataFrame): MLPPOutcomesImplicits =
    new MLPPOutcomesImplicits(dataFrame)

  def makeOutcomes(enhancedEvents: DataFrame): DataFrame = {
    enhancedEvents
      .where(col("category") === Outcome.category)
      .withDiseaseType
      .withDiseaseBucket(params.keepFirstOnly, bucketCount)
      .withIndices(Seq("diseaseType"))
  }

  def writeOutcomes(enhancedEvents: DataFrame): Unit = {
    val outcomes = makeOutcomes(enhancedEvents).persist()
    // write outcomes ("Y" matrices)
    outcomes.makeOutcomes(params.featuresAsList, bucketCount).writeCSV(s"${params.outputRootPath}/csv/Outcomes.csv")
    outcomes.makeStaticOutcomes.writeCSV(s"${params.outputRootPath}/csv/StaticOutcomes.csv")
    outcomes.writeLookupFiles(params.outputRootPath.toString)
    outcomes.unpersist()
  }

}