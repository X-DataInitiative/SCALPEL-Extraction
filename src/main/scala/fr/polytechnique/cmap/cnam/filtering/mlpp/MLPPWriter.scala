package fr.polytechnique.cmap.cnam.filtering.mlpp

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.FlatEvent
import fr.polytechnique.cmap.cnam.utilities.ColumnUtilities._
import fr.polytechnique.cmap.cnam.utilities.functions._

object MLPPWriter {

  final val AgeReferenceDate = makeTS(2006, 12, 31, 23, 59, 59)

  case class Params(
    bucketSize: Int = 30,
    lagCount: Int = 10,
    minTimestamp: Timestamp = makeTS(2006, 1, 1),
    maxTimestamp: Timestamp = makeTS(2009, 12, 31, 23, 59, 59)
  )

  def apply(params: Params = Params()) = new MLPPWriter(params)
}

class MLPPWriter(params: MLPPWriter.Params = MLPPWriter.Params()) {

  import MLPPWriter._

  val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt

  implicit class MLPPDataFrame(data: DataFrame) {

    import data.sqlContext.implicits._

    def withAge(referenceDate: Timestamp = AgeReferenceDate): DataFrame = {
      val age: Column = year(lit(referenceDate)) - year(col("birthDate"))
      data.withColumn("age", age)
    }

    // TODO: merge this function with withDeathBucket into a generic one (withBucketizedColumn(colName))
    def withStartBucket: DataFrame = {
      data.withColumn("startBucket",
        col("start").bucketize(params.minTimestamp, params.maxTimestamp, params.bucketSize)
      )
    }

    def withDeathBucket: DataFrame = {
      data.withColumn("deathBucket",
        col("deathDate").bucketize(params.minTimestamp, params.maxTimestamp, params.bucketSize)
      )
    }

    def withDiseaseBucket: DataFrame = {
      val window = Window.partitionBy("patientId")

      val diseaseBucket: Column = min(
        when(col("category") === "disease" && col("eventId") === "targetDisease", col("startBucket"))
      ).over(window)

      data.withColumn("diseaseBucket", diseaseBucket)
    }

    def withEndBucket: DataFrame = {

      val endBucket: Column = minColumn(
        col("diseaseBucket"), col("deathBucket"), lit(bucketCount)
      )
      data.withColumn("endBucket", endBucket)
    }

    def withIndices(columnNames: Seq[String]): DataFrame = {

      columnNames.foldLeft(data){
        (data, columnName) => {

          // WARNING: The following code is dangerous, but there is no perfect solution.
          //   It was tested with up to 30 million distinct values with 15 characters and 300
          //   million total rows. If we have more than that, we may need to fall back to a join
          //   option, which would take very much longer.
          val labels = data
            .select(col(columnName).cast(StringType))
            .distinct
            .map(_.getString(0))
            .collect
            .toSeq
            .sorted

          // It's transient to avoid serializing the full Map. Only the labels Seq will be
          // serialized and each executor will compute their own Map. This is slower but allows more
          // labels to be indexed.
          @transient lazy val indexMap = labels.zipWithIndex.toMap

          val indexer = udf(indexMap.apply _)

          data.withColumn(columnName + "Index", indexer(col(columnName)))
        }
      }
    }

    def makeDiscreteExposures: Dataset[LaggedExposure] = {
      data
        .groupBy(
          "patientID", "patientIDIndex", "gender", "age", "molecule", "moleculeIndex", "startBucket",
          "endBucket"
        ).agg(
          lit(0).as("lag"),
          lit(1.0).as("weight") // In the future, we might change it to sum("weight").as("weight")
        )
        .as[LaggedExposure]
    }
  }

  implicit class DiscreteExposures(exposures: Dataset[LaggedExposure]) {

    import exposures.sqlContext.implicits._

    def lagExposures: Dataset[LaggedExposure] = {
      val lagCount =  params.lagCount
      val maxLag = lagCount - 1

      // The following function transforms a single initial exposure like (Pat1, MolA, 4, 0, 1)
      //   into a sequence of lagged exposures like:
      //
      //     Patient  Molecule  Bucket  Lag  Value
      //      Pat1      MolA      4      0     1
      //      Pat1      MolA      5      1     1
      //      Pat1      MolA      6      2     1
      //
      // Basically, this means we are filling the diagonal for the given exposure, stopping
      //   when we reach either the last lag or the defined end bucket (min among bucket count,
      //   death date and target disease date)
      val createLags: (LaggedExposure) => Seq[LaggedExposure] = {
        exposure => (0 to maxLag).collect {
          case newLag if exposure.startBucket + newLag < exposure.endBucket =>
            exposure.copy(startBucket = exposure.startBucket + newLag, lag = newLag)
          }
      }

      exposures.flatMap(createLags)
    }

    def toMLPPFeatures: Dataset[MLPPFeature] = {
      // We need a copy of the fields to avoid serialization of the whole class
      val bucketsCount = bucketCount
      val lagCount = params.lagCount
      exposures.map(MLPPFeature.fromLaggedExposure(_, bucketsCount, lagCount))
    }

    def makeStaticExposures: DataFrame = {
      val exposuresDF = exposures.toDF

      val referenceColumnNames = List("age", "gender", "patientID", "patientIDIndex")
      val referenceColumns = referenceColumnNames.map(col)

      // We rename the molecule values considering their index, so we can order the columns later.
      //   The name will be in the format "MOLXXXX_MoleculeName", where XXXX is its left-padded
      //   index.
      val moleculeColumn: Column = concat(
        lit("MOL"),
        lpad(col("moleculeIndex").cast(StringType), 4, "0"),
        lit("_"),
        col("molecule")
      )

      val pivoted = exposuresDF
        .withColumn("toPivot", moleculeColumn)
        .groupBy(referenceColumns: _*)
        .pivot("toPivot").agg(coalesce(sum("weight"), lit(0D)))

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
  }

  // Maybe put this in an implicit class of Dataset[FlatEvent]? This would cause the need of the following:
  //   val writer = MLPPWriter(params)
  //   import writer.FlatEventDataset
  //   data.write(path)
  //
  // Returns the final dataset for convenience
  def write(data: Dataset[FlatEvent], path: String): Dataset[MLPPFeature] = {

    val rootDir = if(path.last == '/') path.dropRight(1) else path
    val input = data.toDF

    val initialExposures = input
      .withAge(AgeReferenceDate)
      .withStartBucket
      .withDiseaseBucket
      .withDeathBucket
      .withEndBucket
      .where(col("category") === "exposure")
      .withColumnRenamed("eventId", "molecule")
      .where(col("startBucket") < col("endBucket"))
      .withIndices(Seq("patientID", "molecule"))
      .makeDiscreteExposures

    val StaticExposures = initialExposures.makeStaticExposures
    StaticExposures.write.parquet(s"$rootDir/StaticExposures")

    val result = initialExposures
      .lagExposures
      .toMLPPFeatures

    result.toDF.write.parquet(s"$rootDir/SparseFeatures")
    result
  }
}
