package fr.polytechnique.cmap.cnam.filtering.mlpp

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.{FilteringConfig, FlatEvent}
import fr.polytechnique.cmap.cnam.utilities.ColumnUtilities._
import fr.polytechnique.cmap.cnam.utilities.functions._

object MLPPWriter {

  final val AgeReferenceDate = FilteringConfig.dates.ageReference

  case class Params(
    bucketSize: Int = 30,
    lagCount: Int = 10,
    minTimestamp: Timestamp = makeTS(2006, 1, 1),
    maxTimestamp: Timestamp = makeTS(2009, 12, 31, 23, 59, 59),
    includeDeathBucket: Boolean = false
  )

  def apply(params: Params = Params()) = new MLPPWriter(params)
}

class MLPPWriter(params: MLPPWriter.Params = MLPPWriter.Params()) {

  import MLPPWriter._

  val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt

  implicit class MLPPDataFrame(data: DataFrame) {

    import data.sparkSession.implicits._

    def withAge(referenceDate: Timestamp = AgeReferenceDate): DataFrame = {
      val age: Column = year(lit(referenceDate)) - year(col("birthDate"))
      data.withColumn("age", age)
    }

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

    def withTracklossBucket: DataFrame = {
      val window = Window.partitionBy("patientId")

      val hadTrackloss: Column = (col("category") === "trackloss") &&
        (col("startBucket") < minColumn(col("deathBucket"), lit(bucketCount)))

      val tracklossBucket: Column = min(when(hadTrackloss, col("startBucket"))).over(window)

      data.withColumn("tracklossBucket", tracklossBucket)
    }

    def withDiseaseBucket: DataFrame = {
      val window = Window.partitionBy("patientId")

      val hadDisease: Column = (col("category") === "disease") &&
      (col("eventId") === "targetDisease") &&
      (col("startBucket") < minColumn(col("deathBucket"), lit(bucketCount)))

      val diseaseBucket: Column = min(when(hadDisease, col("startBucket"))).over(window)

      data.withColumn("diseaseBucket", diseaseBucket)
    }

    // We are no longer using trackloss and disease information for calculating the end bucket.
    def withEndBucket: DataFrame = {

      val deathBucketRule = if (params.includeDeathBucket) col("deathBucket") + 1 else col("deathBucket")

      val endBucket: Column = minColumn(deathBucketRule, lit(bucketCount))
      data.withColumn("endBucket", endBucket)
    }

    def withIndices(columnNames: Seq[String]): DataFrame = {

      val dataCopy = data
      columnNames.foldLeft(dataCopy){
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

    def makeDiscreteExposures: Dataset[LaggedExposure] = {

      val discreteColumns: Seq[Column] = Seq("patientID", "patientIDIndex", "gender", "age",
        "diseaseBucket", "molecule", "moleculeIndex", "startBucket", "endBucket").map(col)

      data
        // In the future, we might change it to sum("weight").as("weight")
        .groupBy(discreteColumns: _*).agg(lit(0).as("lag"), lit(1.0).as("weight"))
        .as[LaggedExposure]
    }

    def writeCSV(path: String): Unit = {
      data.coalesce(1).write
        .format("csv")
        .option("delimiter", ",")
        .option("header", "true")
        .save(path)
    }
  }

  implicit class DiscreteExposures(exposures: Dataset[LaggedExposure]) {

    import exposures.sqlContext.implicits._

    def makeMetadata: Dataset[Metadata] = {
      // We already have this information inside the "withIndices" function, in the labels object,
      //   however, I couldn't think of a good readable solution to extract this information to
      //   the outer scope, so I just compute it again here.
      val max = math.max _
      val patientCount = exposures.map(_.patientIDIndex).reduce(max) + 1
      val moleculeCount = exposures.map(_.moleculeIndex).reduce(max) + 1
      val lags = params.lagCount
      val buckets = bucketCount
      val bucketSize = params.bucketSize

      Seq(
        Metadata(
          rows = patientCount * buckets,
          columns = moleculeCount * lags,
          patients = patientCount,
          buckets = buckets,
          bucketSize = bucketSize,
          molecules = moleculeCount,
          lags = lags
        )
      ).toDS
    }

    def lagExposures: Dataset[LaggedExposure] = {
      val lagCount =  params.lagCount // to avoid full class serialization

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
        e: LaggedExposure => (0 until lagCount).collect {
          case newLag if e.startBucket + newLag < e.endBucket =>
            e.copy(startBucket = e.startBucket + newLag, lag = newLag)
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

    // The following function assumes the data has been filtered and contains only patients with the
    //   disease
    def makeOutcomes: DataFrame = {
      val b = bucketCount
      exposures.map(
        e => e.patientIDIndex * b + e.diseaseBucket.get
      ).distinct.toDF
    }

    // The following function assumes the data has been filtered and contains only patients with the
    //   disease
    def makeStaticOutcomes: DataFrame = {
      exposures.map(_.patientIDIndex).distinct.toDF
    }

    def writeLookupFiles(rootDir: String): Unit = {
      val inputDF = exposures.toDF

      inputDF
        .select("patientID", "patientIDIndex", "gender", "age")
        .dropDuplicates(Seq("patientID"))
        .writeCSV(s"$rootDir/csv/PatientsLookup.csv")

      inputDF
        .select("molecule", "moleculeIndex")
        .dropDuplicates(Seq("molecule"))
        .writeCSV(s"$rootDir/csv/MoleculeLookup.csv")
    }
  }

  // Maybe put this in an implicit class of Dataset[FlatEvent]? This would cause the need of the following:
  //   val writer = MLPPWriter(params)
  //   import writer.FlatEventDataset (or import writer._)
  //   data.write(path)
  //
  // Returns the features dataset for convenience
  def write(data: Dataset[FlatEvent], path: String): Dataset[MLPPFeature] = {

    val rootDir = if(path.last == '/') path.dropRight(1) else path
    val input = data.toDF

    val initialExposures: Dataset[LaggedExposure] = input
      .withAge(AgeReferenceDate)
      .withStartBucket
      .withDeathBucket
      .withDiseaseBucket
      .withEndBucket
      .where(col("category") === "exposure")
      .withColumnRenamed("eventId", "molecule")
      .where(col("startBucket") < col("endBucket"))
      .withIndices(Seq("patientID", "molecule"))
      .makeDiscreteExposures
      .persist()

    val filteredExposures = initialExposures.filter(_.diseaseBucket.isDefined).persist()

    val metadata: DataFrame = initialExposures.makeMetadata.toDF
    val staticExposures: DataFrame = initialExposures.makeStaticExposures
    val outcomes = filteredExposures.makeOutcomes
    val staticOutcomes = filteredExposures.makeStaticOutcomes
    val features: Dataset[MLPPFeature] = filteredExposures.lagExposures.toMLPPFeatures
    val featuresDF = features.toDF.persist()

    // write static exposures ("Z" matrix)
    staticExposures.write.parquet(s"$rootDir/parquet/StaticExposures")
    staticExposures.writeCSV(s"$rootDir/csv/StaticExposures.csv")
    // write outcomes ("Y" matrices)
    outcomes.writeCSV(s"$rootDir/csv/Outcomes.csv")
    staticOutcomes.writeCSV(s"$rootDir/csv/StaticOutcomes.csv")
    // write lookup tables
    initialExposures.writeLookupFiles(rootDir)
    // write sparse features ("X" matrix)
    featuresDF.write.parquet(s"$rootDir/parquet/SparseFeatures")
    featuresDF.select("rowIndex", "colIndex", "value").writeCSV(s"$rootDir/csv/SparseFeatures.csv")

    // write metadata
    metadata.writeCSV(s"$rootDir/csv/metadata.csv")

    featuresDF.unpersist()
    filteredExposures.unpersist()
    initialExposures.unpersist()

    // Return the features for convenience
    features
  }
}
