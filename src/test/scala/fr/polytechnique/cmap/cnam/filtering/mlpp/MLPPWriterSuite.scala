package fr.polytechnique.cmap.cnam.filtering.mlpp

import org.apache.spark.sql.Dataset
import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.FlatEvent
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class MLPPWriterSuite extends SharedContext {

  "withAge" should "add a column with the patients age at the reference date" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", makeTS(1950, 1, 1)),
      ("Patient_A", makeTS(1950, 1, 1)),
      ("Patient_B", makeTS(1960, 7, 15)),
      ("Patient_B", makeTS(1960, 7, 15)),
      ("Patient_C", makeTS(1970, 12, 31)),
      ("Patient_C", makeTS(1970, 12, 31)),
      ("Patient_D", makeTS(1970, 12, 31)),
      ("Patient_D", makeTS(1970, 12, 31))
    ).toDF("patientID", "birthDate")

    val expected = Seq(
      ("Patient_A", makeTS(1950, 1, 1), 56),
      ("Patient_A", makeTS(1950, 1, 1), 56),
      ("Patient_B", makeTS(1960, 7, 15), 46),
      ("Patient_B", makeTS(1960, 7, 15), 46),
      ("Patient_C", makeTS(1970, 12, 31), 36),
      ("Patient_C", makeTS(1970, 12, 31), 36),
      ("Patient_D", makeTS(1970, 12, 31), 36),
      ("Patient_D", makeTS(1970, 12, 31), 36)
    ).toDF("patientID", "birthDate", "age")

    // When
    val writer = MLPPWriter()
    import writer.MLPPDataFrame
    val result = input.withAge()

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withStartBucket" should "add a column with the start date bucket of the event" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2
    )

    val input = Seq(
      ("PA", makeTS(2006, 1, 1))
    ).toDF("patientID", "start")

    val expected = Seq(
      ("PA", makeTS(2006, 1, 1), 0)
    ).toDF("patientID", "start", "startBucket")

    // When
    val writer = MLPPWriter(params)
    import writer.MLPPDataFrame
    val result = input.withStartBucket

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withDeathBucket" should "add a column with the death date bucket of the patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2
    )

    val input = Seq(
      ("PA", Some(makeTS(2006, 1, 1))),
      ("PA", None)
    ).toDF("patientID", "deathDate")

    val expected = Seq(
      ("PA", Some(makeTS(2006, 1, 1)), Some(0)),
      ("PA", None, None)
    ).toDF("patientID", "deathDate", "deathBucket")

    // When
    val writer = MLPPWriter(params)
    import writer.MLPPDataFrame
    val result = input.withDeathBucket

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withDiseaseBucket" should "add a column with the timeBucket of the first targetDisease of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("PA", "molecule", "PIOGLITAZONE", 0),
      ("PA", "molecule", "PIOGLITAZONE", 5),
      ("PA", "disease", "targetDisease", 3),
      ("PB", "molecule", "PIOGLITAZONE", 2),
      ("PB", "disease", "targetDisease", 4),
      ("PC", "molecule", "PIOGLITAZONE", 0)
    ).toDF("patientID", "category", "eventId", "startBucket")

    val expected = Seq(
      ("PA", "molecule", "PIOGLITAZONE", 0, Some(3)),
      ("PA", "molecule", "PIOGLITAZONE", 5, Some(3)),
      ("PA", "disease", "targetDisease", 3, Some(3)),
      ("PB", "molecule", "PIOGLITAZONE", 2, Some(4)),
      ("PB", "disease", "targetDisease", 4, Some(4)),
      ("PC", "molecule", "PIOGLITAZONE", 0, None)
    ).toDF("patientID", "category", "eventId", "startBucket", "diseaseBucket")

    // When
    val writer = MLPPWriter()
    import writer.MLPPDataFrame
    val result = input.withDiseaseBucket

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withEndBucket" should "add a column with the minimum among deathBucket, diseaseBucket and the max number of buckets" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2
    )

    val input = Seq(
      ("PA", Some(2), Some(3)),
      ("PA", Some(2), Some(3)),
      ("PB", Some(4), Some(3)),
      ("PB", Some(4), Some(3)),
      ("PC", None, Some(4)),
      ("PC", None, Some(4)),
      ("PD", Some(4), None),
      ("PD", Some(4), None),
      ("PE", None, None)
    ).toDF("patientID", "deathBucket", "diseaseBucket")

    val expected = Seq(
      ("PA", Some(2)),
      ("PA", Some(2)),
      ("PB", Some(3)),
      ("PB", Some(3)),
      ("PC", Some(4)),
      ("PC", Some(4)),
      ("PD", Some(4)),
      ("PD", Some(4)),
      ("PE", Some(16))
    ).toDF("patientID", "endBucket")

    // When
    val writer = MLPPWriter(params)
    import writer.MLPPDataFrame
    val result = input.withEndBucket.select("patientID", "endBucket")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "makeDiscreteExposures" should "return a Dataset containing the 0-lag exposures in the sparse format" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("PA", 0, 1, 75, "Mol1", 0, 0, 6),
      ("PA", 0, 1, 75, "Mol1", 0, 0, 6),
      ("PA", 0, 1, 75, "Mol1", 0, 2, 6),
      ("PA", 0, 1, 75, "Mol2", 1, 3, 6),
      ("PB", 1, 2, 40, "Mol1", 0, 0, 8),
      ("PB", 1, 2, 40, "Mol1", 0, 4, 8),
      ("PB", 1, 2, 40, "Mol1", 0, 4, 8),
      ("PB", 1, 2, 40, "Mol2", 1, 6, 8)
    ).toDF("patientID", "patientIDIndex", "gender", "age", "molecule", "moleculeIndex", "startBucket", "endBucket")

    val expected = Seq(
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol2", 1, 3, 6, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 4, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol2", 1, 6, 8, 0, 1.0)
    ).toDF

    // When
    val writer = MLPPWriter()
    import writer.MLPPDataFrame
    val result = input.makeDiscreteExposures.toDF

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withIndices" should "add a column with the indices of each given column" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("PA", "Mol1"),
      ("PA", "Mol2"),
      ("PA", "Mol2"),
      ("PB", "Mol1"),
      ("PB", "Mol1"),
      ("PC", "Mol3")
    ).toDF("patientID", "molecule")
    val expected = Seq(
      ("PA", "Mol1", 0, 0),
      ("PA", "Mol2", 0, 1),
      ("PA", "Mol2", 0, 1),
      ("PB", "Mol1", 1, 0),
      ("PB", "Mol1", 1, 0),
      ("PC", "Mol3", 2, 2)
    ).toDF("patientID", "molecule", "patientIDIndex", "moleculeIndex")

    // When
    val writer = MLPPWriter()
    import writer.MLPPDataFrame
    val result = input.withIndices(Seq("patientID", "molecule")).toDF

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  it should "still work with larger data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val rdd = sc.parallelize(for(i <- 0 until 50000; j <- 1 to 10) yield (i, (i % 1000)))
    val input = rdd.map(i => (f"${i._1}%015d", f"${i._2}%03d")).toDF("pID", "mol")
    val expected = rdd.map(i => (i._1, i._2 )).toDF("pIDIndex", "molIndex")

    // When
    val writer = MLPPWriter()
    import writer.MLPPDataFrame
    val result = input.withIndices(Seq("pID", "mol")).select("pIDIndex", "molIndex")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "lagExposures" should "create the lagged elements of the matrix for each exposure" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(lagCount = 4)
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol2", 1, 4, 6, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 4, 8, 0, 1.0)
    ).toDS

    val expected = Seq(
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 1, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 2, 6, 2, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 3, 6, 3, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 3, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 4, 6, 2, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 5, 6, 3, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol2", 1, 4, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol2", 1, 5, 6, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 1, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 2, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 3, 8, 3, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 4, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 5, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 6, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 7, 8, 3, 1.0)
    ).toDF

    // When
    val writer = MLPPWriter(params)
    import writer.DiscreteExposures
    val result = input.lagExposures.toDF

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "toMLPPFeatures" should "create a new Dataset with the final COO sparse matrix format" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2,
      lagCount = 4
    )
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 1, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 2, 6, 2, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 3, 6, 3, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 3, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 4, 6, 2, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 5, 6, 3, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol2", 1, 4, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol2", 1, 5, 6, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 1, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 2, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 3, 8, 3, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 4, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 5, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 6, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 7, 8, 3, 1.0)
    ).toDS

    val expected = Seq(
      // pId, pIdx, mol, molIdx, bkt, lag, row, col, val
      MLPPFeature("PA", 0, "Mol1", 0, 0, 0,  0, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 1, 1,  1, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 2,  2, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 3,  3, 3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 0,  2, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 1,  3, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 2,  4, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 3,  5, 3, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 4, 0,  4, 4, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 5, 1,  5, 5, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 0, 0, 16, 0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 1, 1, 17, 1, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 2, 2, 18, 2, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 3, 3, 19, 3, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 4, 0, 20, 0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 5, 1, 21, 1, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 6, 2, 22, 2, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 7, 3, 23, 3, 1.0)
    ).toDF

    // When
    val writer = MLPPWriter(params)
    import writer.DiscreteExposures
    val result = input.toMLPPFeatures.toDF

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "makeStaticExposures" should "compute the static exposures matrix in a DataFrame format" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, "ROS", 2, 0, 1, 0, 1),
      LaggedExposure("PA", 0, 1, 75, "PIO", 1, 1, 2, 0, 1),
      LaggedExposure("PA", 0, 1, 75, "ROS", 2, 2, 3, 0, 1),
      LaggedExposure("PB", 1, 2, 40, "INS", 0, 1, 2, 0, 1),
      LaggedExposure("PC", 2, 1, 50, "ROS", 2, 0, 1, 0, 1)
    ).toDS()

    val expected = Seq(
      (0D, 1D, 2D, 75, 1, "PA", 0),
      (1D, 0D, 0D, 40, 2, "PB", 1),
      (0D, 0D, 1D, 50, 1, "PC", 2)
    ).toDF("MOL0000_INS", "MOL0001_PIO", "MOL0002_ROS", "age", "gender", "patientID", "patientIDIndex")

    // When
    val writer = MLPPWriter()
    import writer.DiscreteExposures
    val result = input.makeStaticExposures

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "write" should "create the final matrices and write them as parquet files" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val rootDir = "target/test/output"
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 8, 1), // 7 total buckets
      bucketSize = 30,
      lagCount = 4
    )
    val input: Dataset[FlatEvent] = Seq(
      FlatEvent("PC", 2, makeTS(1970, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 5, 15), None),
      FlatEvent("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 6, 15)), "exposure", "Mol1", 1.0, makeTS(2006, 1, 15), None),
      FlatEvent("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 6, 15)), "exposure", "Mol2", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 6, 15)), "exposure", "Mol2", 1.0, makeTS(2006, 5, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 1, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 4, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol2", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol3", 1.0, makeTS(2006, 4, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "disease", "targetDisease", 1.0, makeTS(2006, 5, 15), None)
    ).toDS

    val expectedFeatures = Seq(
      // Patient A
      MLPPFeature("PA", 0, "Mol1", 0,  0, 0,  0, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0,  1, 1,  1, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0,  2, 2,  2, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0,  3, 3,  3, 3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0,  2, 0,  2, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0,  3, 1,  3, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0,  3, 0,  3, 0, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1,  2, 0,  2, 4, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1,  3, 1,  3, 5, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2,  3, 0,  3, 8, 1.0),
      // Patient B
      MLPPFeature("PB", 1, "Mol1", 0,  0, 0,  7, 0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0,  1, 1,  8, 1, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0,  2, 2,  9, 2, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0,  3, 3, 10, 3, 1.0),
      MLPPFeature("PB", 1, "Mol2", 1,  2, 0,  9, 4, 1.0),
      MLPPFeature("PB", 1, "Mol2", 1,  3, 1, 10, 5, 1.0),
      MLPPFeature("PB", 1, "Mol2", 1,  4, 2, 11, 6, 1.0),
      MLPPFeature("PB", 1, "Mol2", 1,  4, 0, 11, 4, 1.0),
      // Patient C
      MLPPFeature("PC", 2, "Mol1", 0,  4, 0, 18, 0, 1.0),
      MLPPFeature("PC", 2, "Mol1", 0,  5, 1, 19, 1, 1.0),
      MLPPFeature("PC", 2, "Mol1", 0,  6, 2, 20, 2, 1.0)
    ).toDF

    val expectedZMatrix = Seq(
      (3D, 1D, 1D, 46, 1, "PA", 0),
      (1D, 2D, 0D, 56, 1, "PB", 1),
      (1D, 0D, 0D, 36, 2, "PC", 2)
    ).toDF("MOL0000_Mol1", "MOL0001_Mol2", "MOL0002_Mol3", "age", "gender", "patientID", "patientIDIndex")

    // When
    val result = MLPPWriter(params).write(input, rootDir).toDF
    val writtenResult = sqlContext.read.parquet(s"$rootDir/SparseFeatures")
    val StaticExposures = sqlContext.read.parquet(s"$rootDir/StaticExposures")

    // Then
    import RichDataFrames._
    result.show
    expectedFeatures.show
    StaticExposures.show
    expectedZMatrix.show
    assert(result === expectedFeatures)
    assert(writtenResult === expectedFeatures)
    assert(StaticExposures === expectedZMatrix)
  }
}
