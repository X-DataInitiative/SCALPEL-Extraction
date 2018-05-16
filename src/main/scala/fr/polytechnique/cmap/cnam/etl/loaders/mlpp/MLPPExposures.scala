package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events.Exposure
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.MLPPLoader.Params
import fr.polytechnique.cmap.cnam.util.RichDataFrame._
import fr.polytechnique.cmap.cnam.util.functions.daysBetween


class MLPPExposuresImplicits(data: DataFrame) {
  def toDiscreteExposures: Dataset[LaggedExposure] = {
    import data.sqlContext.implicits._

    val discreteColumns: Seq[Column] = Seq(
      "patientID", "patientIDIndex", "gender", "age", "exposureType", "exposureTypeIndex", "startBucket", "endBucket"
    ).map(col)

    data
      // In the future, we might change it to sum("weight").as("weight")
      .groupBy(discreteColumns: _*).agg(lit(0).as("lag"), lit(1.0).as("weight"))
      .as[LaggedExposure]
  }
}

class MLPPExposures(params: Params) {

  private val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt

  implicit def toMLPPExposures(dataFrame: DataFrame): MLPPExposuresImplicits = new MLPPExposuresImplicits(dataFrame)

  def makeExposures(enhancedEvents: DataFrame): Dataset[LaggedExposure] = {
    import enhancedEvents.sqlContext.implicits._
    enhancedEvents
      .where((col("category") === Exposure.category) && (col("startBucket") < col("endBucket")))
      .withColumnRenamed("value", "exposureType")
      .withIndices(Seq("exposureType"))
      .toDiscreteExposures
      .as[LaggedExposure]
  }

  def writeExposures(exposures: Dataset[LaggedExposure]): Dataset[MLPPFeature] = {
    import DiscreteExposure._
    val staticExposures: DataFrame = exposures.makeStaticExposures.persist()
    // write static exposures ("Z" matrix)
    staticExposures.write.parquet(s"${params.outputRootPath}/parquet/StaticExposures")
    staticExposures.writeCSV(s"${params.outputRootPath}/csv/StaticExposures.csv")
    staticExposures.unpersist()

    // Make MLPP ready features
    val features: Dataset[MLPPFeature] = exposures.lagExposures(params.lagCount).toMLPPFeatures(params.lagCount, bucketCount)

    val featuresDF = features.toDF().persist()
    // write sparse features ("X" matrix)
    featuresDF.write.parquet(s"${params.outputRootPath}/parquet/SparseFeatures")
    featuresDF.writeCSV(s"${params.outputRootPath}/csv/SparseFeatures.csv")
    featuresDF.unpersist()

    // write lookup tables
    exposures.writeLookupFiles(params.outputRootPath.toString)
    features
  }

}