package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.ColumnUtilities._
import fr.polytechnique.cmap.cnam.util.functions._


class MLPPLoader(params: MLPPLoader.Params = MLPPLoader.Params()) {

  private val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt

  implicit class MLPPDataFrame(data: DataFrame) {

    import data.sparkSession.implicits._

    def withAge: DataFrame = {
      val age: Column = year(lit(params.minTimestamp)) - year(col("birthDate"))
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

    // TODO : This one should also disappear since we compute the end date in the Exposure transformer
    // We are no longer using disease information for calculating the end bucket.
    def withEndBucket: DataFrame = {

      val deathBucketRule = if (params.includeCensoredBucket) col("deathBucket") + 1 else col("deathBucket")
      val tracklossBucketRule = if (params.includeCensoredBucket) col("tracklossBucket") + 1 else col("tracklossBucket")

      val endBucket: Column = minColumn(deathBucketRule, tracklossBucketRule, lit(bucketCount))
      data.withColumn("endBucket", endBucket)
    }

    def withIndices(columnNames: Seq[String]): DataFrame = {

      columnNames.foldLeft(data){
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

    def toDiscreteExposures: Dataset[LaggedExposure] = {

      val discreteColumns: Seq[Column] = Seq("patientID", "patientIDIndex", "gender", "age",
        "exposureType", "exposureTypeIndex", "startBucket", "endBucket").map(col)

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

  implicit class MLPPOutcomesDataFrame(outcomes: DataFrame) {

    def withDiseaseType: DataFrame = {
      outcomes.withColumnRenamed("groupID", "diseaseType")
    }

    // TODO: Find a solution for the keep first only part
    // This function has two different tricky behaviours that are misleading based on the parameter passed.
    // If keepFirstOnly is passed than it creates a window over patientID et diseaseType
    def withDiseaseBucket(keepFirstOnly: Boolean): DataFrame = {
      if (keepFirstOnly) {
        val window = Window.partitionBy("patientID", "diseaseType")

        val hadDisease: Column = (col("category") === Outcome.category) &&
          (col("startBucket") < minColumn(col("endBucket"), lit(bucketCount)))

        val diseaseBucket: Column = min(when(hadDisease, col("startBucket"))).over(window)

        outcomes.withColumn("diseaseBucket", diseaseBucket)
      }
      else {
        outcomes.withColumn(
          "diseaseBucket", when(
            col("category") === Outcome.category &&
              (col("startBucket") < minColumn(col("endBucket"), lit(bucketCount))), col("startBucket")
          )
        )
      }
    }

    def makeOutcomes: DataFrame = {
      if (params.featuresAsList) {
        outcomes
          .select(Seq(col("patientIDIndex"), col("diseaseBucket"), col("diseaseTypeIndex")): _*)
          .distinct.toDF("patientIndex", "bucket", "diseaseType")
      }
      else {
        val b = bucketCount
        outcomes
          .withColumn("patientBucketIndex", col("patientIDIndex") * b + col("diseaseBucket"))
          .select(Seq(col("patientBucketIndex"), col("diseaseTypeIndex")): _*)
          .distinct
      }
    }

    // The following function assumes the data has been filtered and contains only patients with the disease
    def makeStaticOutcomes: DataFrame = {
      outcomes
        .select(Seq(col("patientIDIndex"), col("diseaseTypeIndex")): _*)
        .distinct
        .toDF("patientIDIndex", "diseaseType")
    }

    def writeLookupFiles(rootDir: String): Unit = {
      val inputDF = outcomes.toDF

      inputDF
        .select("diseaseType", "diseaseTypeIndex")
        .dropDuplicates(Seq("diseaseType"))
        .writeCSV(s"$rootDir/csv/OutcomesLookup.csv")
    }
  }

  implicit class DiscreteExposures(exposures: Dataset[LaggedExposure]) {

    import exposures.sqlContext.implicits._

    def makeMetadata: Metadata = {
      // We already have this information inside the "withIndices" function, in the labels object,
      //   however, I couldn't think of a good readable solution to extract this information to
      //   the outer scope, so I just compute it again here.
      val max = math.max _
      val patientCount = exposures.map(_.patientIDIndex).reduce(max) + 1
      val exposureTypeCount = exposures.map(_.exposureTypeIndex).reduce(max) + 1
      val lags = params.lagCount
      val buckets = bucketCount
      val bucketSize = params.bucketSize

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

    def lagExposures: Dataset[LaggedExposure] = {
      val lagCount =  params.lagCount // to avoid full class serialization

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

    def makeCensoring: DataFrame = {
      val b = bucketCount
      val filtered = exposures.filter(_.endBucket < b)
      if(params.featuresAsList) {
        filtered
          .map(e => (e.patientIDIndex, e.endBucket))
          .distinct.toDF("patientIndex", "bucket")
      }
      else {
        filtered.map(e => e.patientIDIndex * b + e.endBucket).distinct.toDF("index")
      }
    }

    def writeLookupFiles(rootDir: String): Unit = {
      val inputDF = exposures.toDF

      inputDF
        .select("patientID", "patientIDIndex", "gender", "age")
        .dropDuplicates(Seq("patientID"))
        .writeCSV(s"$rootDir/csv/PatientsLookup.csv")

      inputDF
        .select("exposureType", "exposureTypeIndex")
        .dropDuplicates(Seq("exposureType"))
        .writeCSV(s"$rootDir/csv/MoleculeLookup.csv")

    }
  }


  /**
    * Builds a Dataset of Patient with patients that have at least one outcome and one expositions
    * @param outcome: Dataset of the Outcomes
    * @param exposure: Dataset of Exposures
    * @param patient: Dataset of patients
    * @return Dataset of patients
    */
  def getFinalPatients(
    outcome: Dataset[Event[Outcome]],
    exposure: Dataset[Event[Exposure]],
    patient: Dataset[Patient]): Dataset[Patient] = {
    val patientsWithOutcome = outcome.select("patientID").distinct().toDF("patientID")
    val patientsWithExposure = exposure.select("patientID").distinct().toDF("patientID")

    val finalPatientsIDs = patientsWithExposure.join(patientsWithOutcome, "patientID")
    import outcome.sqlContext.implicits._
    patient.join(finalPatientsIDs, "patientID").as[Patient]
  }

  // Maybe put this in an implicit class of Dataset[FlatEvent]? This would cause the need of the following:
  //   val writer = MLPPWriter(params)
  //   import writer.FlatEventDataset (or import writer._)
  //   data.write(path)
  //
  // Returns the features dataset for convenience
  def load(
      outcome: Dataset[Event[Outcome]],
      exposure: Dataset[Event[Exposure]],
      patient: Dataset[Patient],
      path: String): Dataset[MLPPFeature] = {

    val rootDir = if(path.last == '/') path.dropRight(1) else path

    val finalPatients = getFinalPatients(outcome, exposure, patient)
    val input = finalPatients.join(outcome.toDF.union(exposure.toDF), "patientID")

    val eventsEnhanced = input
      .withAge
      .withStartBucket
      .withDeathBucket
      .withTracklossBucket
      .withEndBucket
      .withIndices(Seq("patientID"))
      .persist()

    val outcomeEvents = eventsEnhanced
      .where(col("category") === "outcome")
      .withDiseaseType
      .withDiseaseBucket(true)
      .withIndices(Seq("diseaseType"))

    import input.sqlContext.implicits._
    val exposureEvents: Dataset[LaggedExposure] = eventsEnhanced
      .where((col("category") === "exposure") && (col("startBucket") < col("endBucket")))
      .withColumnRenamed("value", "exposureType")
      .withIndices(Seq("exposureType"))
      .toDiscreteExposures
      .as[LaggedExposure]
      .persist()

    // TODO: this should be the last thing to do
    val metadata: Metadata = exposureEvents.makeMetadata
    val staticExposures: DataFrame = exposureEvents.makeStaticExposures

    val censoring = exposureEvents.makeCensoring
    // Based on filtered outcomes, make final outcomes
    val outcomes = outcomeEvents.makeOutcomes
    val staticOutcomes = outcomeEvents.makeStaticOutcomes

    // Make MLPP ready features
    val features: Dataset[MLPPFeature] = exposureEvents.lagExposures.toMLPPFeatures
    val featuresDF = features.toDF.persist()

    // write static exposures ("Z" matrix)
    staticExposures.write.parquet(s"$rootDir/parquet/StaticExposures")
    staticExposures.writeCSV(s"$rootDir/csv/StaticExposures.csv")

    // write Censoring data
    censoring.writeCSV(s"$rootDir/csv/Censoring.csv")

    // write outcomes ("Y" matrices)
    outcomes.writeCSV(s"$rootDir/csv/Outcomes.csv")
    staticOutcomes.writeCSV(s"$rootDir/csv/StaticOutcomes.csv")

    // write lookup tables
    exposureEvents.writeLookupFiles(rootDir)

    // write sparse features ("X" matrix)
    featuresDF.write.parquet(s"$rootDir/parquet/SparseFeatures")
    featuresDF.writeCSV(s"$rootDir/csv/SparseFeatures.csv")

    // write metadata
    import input.sqlContext.implicits._
    Seq(metadata).toDF.writeCSV(s"$rootDir/csv/metadata.csv")

    featuresDF.unpersist()
    exposureEvents.unpersist()

    // Return the features for convenience
    features
  }
}


object MLPPLoader {

  case class Params(
      bucketSize: Int = 1,
      lagCount: Int = 1,
      minTimestamp: Timestamp = makeTS(2006, 1, 1),
      maxTimestamp: Timestamp = makeTS(2009, 12, 31, 23, 59, 59),
      includeCensoredBucket: Boolean = false,
      featuresAsList: Boolean = false)

  def apply(params: Params = Params()) = new MLPPLoader(params)

}
