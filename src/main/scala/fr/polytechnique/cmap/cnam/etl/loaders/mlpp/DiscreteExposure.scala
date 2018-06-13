package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.DiscreteExposure._
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config.MLPPConfig
import fr.polytechnique.cmap.cnam.util.RichDataFrame._
import fr.polytechnique.cmap.cnam.util.functions.daysBetween


class DiscreteExposureImplicits(exposures: Dataset[LaggedExposure]) {

  import exposures.sqlContext.implicits._

  def makeMetadata(lagCount: Int, bucketCount: Int, bucketSize: Int): Metadata = {
    // We already have this information inside the "withIndices" function, in the labels object,
    //   however, I couldn't think of a good readable solution to extract this information to
    //   the outer scope, so I just compute it again here.
    val max = math.max _
    val patientCount = exposures.map(_.patientIDIndex).reduce(max) + 1
    val exposureTypeCount = exposures.map(_.exposureTypeIndex).reduce(max) + 1
    val lags = lagCount
    val buckets = bucketCount

    Metadata(
      rows = patientCount * buckets,
      columns = exposureTypeCount * lags,
      patients = patientCount,
      buckets = buckets,
      bucketSize = bucketSize,
      exposureType = exposureTypeCount,
      lags = lags
    )
  }

  def lagExposures(lagCount: Int): Dataset[LaggedExposure] = {

    // The following function transforms a single initial exposure like (Pat1, MolA, 4, 0, 1)
    //   into a sequence of lagged exposures like:
    //
    //     Patient  ExposureType  Bucket  Lag  Value
    //      Pat1        MolA        4      0     1
    //      Pat1        MolA        5      1     1
    //      Pat1        MolA        6      2     1
    //
    // Basically, this means we are filling the diagonal for the given exposure, stopping
    //   when we reach either the last lag or the defined end bucket (min among bucket count,
    //   death date and target disease date)
    val createLags: (LaggedExposure) => Seq[LaggedExposure] = {
      e: LaggedExposure =>
        (0 until lagCount).collect {
          case newLag if e.startBucket + newLag < e.endBucket =>
            e.copy(startBucket = e.startBucket + newLag, lag = newLag)
        }
    }

    exposures.flatMap(createLags)
  }

  def toMLPPFeatures(lagCount: Int, bucketCount: Int): Dataset[MLPPFeature] = {
    // We need a copy of the fields to avoid serialization of the whole class
    exposures.map(MLPPFeature.fromLaggedExposure(_, bucketCount, lagCount))
  }

  def makeStaticExposures: DataFrame = {
    //avoid a column name which contains a space after a pivot
    val exposuresDF = exposures.toDF.avoidSpecialCharactersBeforePivot("exposureType")

    val referenceColumnNames = List("age", "gender", "patientID", "patientIDIndex")
    val referenceColumns = referenceColumnNames.map(col)

    // We rename the molecule values considering their index, so we can order the columns later.
    //   The name will be in the format "MOLXXXX_MoleculeName", where XXXX is its left-padded
    //   index.
    val exposureTypeColumn: Column = concat(
      lit("MOL"),
      lpad(col("exposureTypeIndex").cast(StringType), 4, "0"),
      lit("_"),
      col("exposureType")
    )

    val pivoted = exposuresDF
      .withColumn("toPivot", exposureTypeColumn)
      .groupBy(referenceColumns: _*)
      .pivot("toPivot").agg(sum("weight")).na.fill(0D)

    // We need to sort the molecule names so we have the column indexes in the right order
    val moleculeColumns: Array[Column] = pivoted.columns
      .filter(!referenceColumnNames.contains(_)).sorted.map(col)

    // Then, we can put the reference columns at the end.
    // The resulting dataframe will have the the following schema:
    // [MOL0000_Molecule0, ..., MOL###K_MoleculeK, age, gender, patientID, patientIDIndex]
    //   so the final Z matrix can be created by sorting by patientIDIndex then dropping the last
    //   two columns.
    pivoted.select(moleculeColumns ++ referenceColumns: _*)
  }

  def makeCensoring(bucketCount: Int, featuresAsList: Boolean): DataFrame = {
    val filtered = exposures.filter(_.endBucket < bucketCount)
    if (featuresAsList) {
      filtered
        .map(e => (e.patientIDIndex, e.endBucket))
        .distinct.toDF("patientIndex", "bucket")
    }
    else {
      filtered.map(e => e.patientIDIndex * bucketCount + e.endBucket).distinct.toDF("index")
    }
  }

  def writeLookupFiles(params: MLPPConfig): Unit = {
    val inputDF = exposures.toDF

    import fr.polytechnique.cmap.cnam.util.RichDataFrame._
    inputDF
      .select("patientID", "patientIDIndex", "gender", "age")
      .dropDuplicates(Seq("patientID"))
      .writeCSV(params.output.patientsLookup)

    inputDF
      .select("exposureType", "exposureTypeIndex")
      .dropDuplicates(Seq("exposureType"))
      .writeCSV(params.output.moleculeLookup)
  }

}

object DiscreteExposure {
  implicit def toDiscreteExposure(dataset: Dataset[LaggedExposure]): DiscreteExposureImplicits =
    new DiscreteExposureImplicits(dataset)
}

class DiscreteExposure(params: MLPPConfig) {

  private val bucketCount = (daysBetween(params.extra.maxTimestamp, params.extra.minTimestamp) / params.base.bucketSize).toInt

  def writeMetadata(exposures: Dataset[LaggedExposure]): Unit = {
    import exposures.sqlContext.implicits._

    Seq(exposures.makeMetadata(params.base.lagCount, bucketCount, params.base.bucketSize))
      .toDF
      .writeCSV(params.output.metadata)
  }

  def writeCensoring(exposures: Dataset[LaggedExposure]): Unit = {
    exposures
      .makeCensoring(bucketCount, params.base.featuresAsList)
      .writeCSV(params.output.censoring)
  }

}
