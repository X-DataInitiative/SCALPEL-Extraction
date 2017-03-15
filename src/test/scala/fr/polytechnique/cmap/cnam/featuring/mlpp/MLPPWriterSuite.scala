package fr.polytechnique.cmap.cnam.featuring.mlpp

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.old_root.FlatEvent
import fr.polytechnique.cmap.cnam.util.functions._

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
    assertDFs(result, expected)
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
    assertDFs(result, expected)
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
    assertDFs(result, expected)
  }

  "withTracklossBucket" should "add a column with the timeBucket of the first trackloss of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("PA", "molecule", "PIOGLITAZONE", 0, Some(4)),
      ("PA", "molecule", "PIOGLITAZONE", 5, Some(4)),
      ("PA", "trackloss", "trackloss",   3, Some(4)),
      ("PB", "molecule", "PIOGLITAZONE", 2,    None),
      ("PB", "trackloss", "trackloss",   4,    None),
      ("PC", "molecule", "PIOGLITAZONE", 0, Some(6)),
      ("PD", "molecule", "PIOGLITAZONE", 2, Some(3)),
      ("PD", "molecule", "PIOGLITAZONE", 3, Some(3)),
      ("PD", "trackloss", "trackloss",   4, Some(3))
    ).toDF("patientID", "category", "eventId", "startBucket", "deathBucket")

    val expected = Seq(
      ("PA", "molecule", "PIOGLITAZONE", 0, Some(4), Some(3)),
      ("PA", "molecule", "PIOGLITAZONE", 5, Some(4), Some(3)),
      ("PA", "trackloss", "trackloss",   3, Some(4), Some(3)),
      ("PB", "molecule", "PIOGLITAZONE", 2,    None, Some(4)),
      ("PB", "trackloss", "trackloss",   4,    None, Some(4)),
      ("PC", "molecule", "PIOGLITAZONE", 0, Some(6),    None),
      ("PD", "molecule", "PIOGLITAZONE", 2, Some(3),    None),
      ("PD", "molecule", "PIOGLITAZONE", 3, Some(3),    None),
      ("PD", "trackloss", "trackloss",   4, Some(3),    None)
    ).toDF("patientID", "category", "eventId", "startBucket", "deathBucket", "tracklossBucket")

    // When
    val writer = MLPPWriter()
    import writer.MLPPDataFrame
    val result = input.withTracklossBucket

    // Then
    assertDFs(result, expected)
  }

  "withDiseaseBucket" should "add a column with the timeBucket of the first targetDisease of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("PA", "molecule", "PIOGLITAZONE", 0, Some(4),    None),
      ("PA", "molecule", "PIOGLITAZONE", 5, Some(4),    None),
      ("PA", "disease", "targetDisease", 3, Some(4),    None),
      ("PB", "molecule", "PIOGLITAZONE", 2,    None, Some(5)),
      ("PB", "disease", "targetDisease", 4,    None, Some(5)),
      ("PC", "molecule", "PIOGLITAZONE", 0, Some(6),    None),
      ("PD", "molecule", "PIOGLITAZONE", 2, Some(3),    None),
      ("PD", "molecule", "PIOGLITAZONE", 3, Some(3),    None),
      ("PD", "disease", "targetDisease", 4, Some(3),    None)
    ).toDF("patientID", "category", "eventId", "startBucket", "deathBucket", "tracklossBucket")

    val expected = Seq(
      ("PA", "molecule", "PIOGLITAZONE", 0, Some(4),    None, Some(3)),
      ("PA", "molecule", "PIOGLITAZONE", 5, Some(4),    None, Some(3)),
      ("PA", "disease", "targetDisease", 3, Some(4),    None, Some(3)),
      ("PB", "molecule", "PIOGLITAZONE", 2,    None, Some(5), Some(4)),
      ("PB", "disease", "targetDisease", 4,    None, Some(5), Some(4)),
      ("PC", "molecule", "PIOGLITAZONE", 0, Some(6),    None,    None),
      ("PD", "molecule", "PIOGLITAZONE", 2, Some(3),    None,    None),
      ("PD", "molecule", "PIOGLITAZONE", 3, Some(3),    None,    None),
      ("PD", "disease", "targetDisease", 4, Some(3),    None,    None)
    ).toDF("patientID", "category", "eventId", "startBucket", "deathBucket", "tracklossBucket", "diseaseBucket")

    // When
    val writer = MLPPWriter()
    import writer.MLPPDataFrame
    val result = input.withDiseaseBucket

    // Then
    assertDFs(result, expected)
  }

  "withEndBucket" should "add a column with the minimum among deathBucket and the max number of buckets" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2
    )

    val input = Seq(
      ("PA", Some(2),    None, Some(3)),
      ("PA", Some(2),    None, Some(3)),
      ("PB", Some(4), Some(5), Some(3)),
      ("PB", Some(4), Some(5), Some(3)),
      ("PC",    None, Some(5), Some(4)),
      ("PC",    None, Some(5), Some(4)),
      ("PD", Some(5),    None, None),
      ("PD", Some(5),    None, None),
      ("PE", Some(7), Some(6), None),
      ("PE", Some(7), Some(6), None),
      ("PF",    None,    None, None)
    ).toDF("patientID", "deathBucket", "tracklossBucket", "diseaseBucket")

    val expected = Seq(
      ("PA", Some(2)),
      ("PA", Some(2)),
      ("PB", Some(4)),
      ("PB", Some(4)),
      ("PC", Some(16)),
      ("PC", Some(16)),
      ("PD", Some(5)),
      ("PD", Some(5)),
      ("PE", Some(7)),
      ("PE", Some(7)),
      ("PF", Some(16))
    ).toDF("patientID", "endBucket")

    // When
    val writer = MLPPWriter(params)
    import writer.MLPPDataFrame
    val result = input.withEndBucket.select("patientID", "endBucket")

    // Then
    assertDFs(result, expected)
  }

  it should "add a column with the minimum among deathBucket + 1, and the max number of buckets if " +
      "includeDeathBucket is true" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2,
      includeDeathBucket = true
    )

    val input = Seq(
      ("PA", Some(16)),
      ("PA", Some(16)),
      ("PB", Some( 0)),
      ("PB", Some( 0)),
      ("PC", Some( 5)),
      ("PC", Some( 5)),
      ("PD",    None),
      ("PD",    None)
    ).toDF("patientID", "deathBucket")

    val expected = Seq(
      ("PA", Some(16)),
      ("PA", Some(16)),
      ("PB", Some( 1)),
      ("PB", Some( 1)),
      ("PC", Some( 6)),
      ("PC", Some( 6)),
      ("PD", Some(16)),
      ("PD", Some(16))
    ).toDF("patientID", "endBucket")

    // When
    val writer = MLPPWriter(params)
    import writer.MLPPDataFrame
    val result = input.withEndBucket.select("patientID", "endBucket")

    // Then
    assertDFs(result, expected)
  }

  "makeDiscreteExposures" should "return a Dataset containing the 0-lag exposures in the sparse format" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("PA", 0, 1, 75, Some(6), "Mol1", 0, 0, 6),
      ("PA", 0, 1, 75, Some(6), "Mol1", 0, 0, 6),
      ("PA", 0, 1, 75, Some(6), "Mol1", 0, 2, 6),
      ("PA", 0, 1, 75, Some(6), "Mol2", 1, 3, 6),
      ("PB", 1, 2, 40,    None, "Mol1", 0, 0, 8),
      ("PB", 1, 2, 40,    None, "Mol1", 0, 4, 8),
      ("PB", 1, 2, 40,    None, "Mol1", 0, 4, 8),
      ("PB", 1, 2, 40,    None, "Mol2", 1, 6, 8)
    ).toDF("patientID", "patientIDIndex", "gender", "age", "diseaseBucket", "molecule", "moleculeIndex", "startBucket", "endBucket")

    val expected = Seq(
      LaggedExposure("PA", 0, 1, 75, Some(6),"Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6),"Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6),"Mol2", 1, 3, 6, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None,"Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None,"Mol1", 0, 4, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None,"Mol2", 1, 6, 8, 0, 1.0)
    ).toDF

    // When
    val writer = MLPPWriter()
    import writer.MLPPDataFrame
    val result = input.makeDiscreteExposures.toDF

    // Then
    assertDFs(result, expected)
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
    assertDFs(result, expected)
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
    assertDFs(result, expected)
  }

  "lagExposures" should "create the lagged elements of the matrix for each exposure" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(lagCount = 4)
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol2", 1, 4, 6, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 4, 8, 0, 1.0)
    ).toDS

    val expected = Seq(
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 1, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 2, 6, 2, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 3, 6, 3, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 3, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 4, 6, 2, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 5, 6, 3, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol2", 1, 4, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol2", 1, 5, 6, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 1, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 2, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 3, 8, 3, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 4, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 5, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 6, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 7, 8, 3, 1.0)
    ).toDF

    // When
    val writer = MLPPWriter(params)
    import writer.DiscreteExposures
    val result = input.lagExposures.toDF

    // Then
    assertDFs(result, expected)
  }

  "makeMetadata" should "create a metadata dataset with some relevant information" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 1),
      bucketSize = 2,
      lagCount = 4
    )
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol3", 2, 1, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol2", 1, 2, 6, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol3", 2, 1, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol4", 3, 6, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol5", 4, 7, 8, 3, 1.0)
    ).toDS
    val expected: Dataset[Metadata] = Seq(
      Metadata(30, 20, 2, 15, 2, 5, 4)
    ).toDS

    // When
    val writer = MLPPWriter(params)
    import writer.DiscreteExposures
    val result = input.makeMetadata

    // Then
    assertDFs(result.toDF, expected.toDF)
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
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 1, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 2, 6, 2, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 3, 6, 3, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 3, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 4, 6, 2, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol1", 0, 5, 6, 3, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol2", 1, 4, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, Some(6), "Mol2", 1, 5, 6, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 1, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 2, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 3, 8, 3, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 4, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 5, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 6, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40,    None, "Mol1", 0, 7, 8, 3, 1.0)
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
    assertDFs(result, expected)
  }

  "makeStaticExposures" should "compute the static exposures matrix in a DataFrame format" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, Some(6), "ROS", 2, 0, 6, 0, 1),
      LaggedExposure("PA", 0, 1, 75, Some(6), "PIO", 1, 1, 6, 0, 1),
      LaggedExposure("PA", 0, 1, 75, Some(6), "ROS", 2, 2, 6, 0, 1),
      LaggedExposure("PB", 1, 2, 40,    None, "INS", 0, 1, 8, 0, 1),
      LaggedExposure("PC", 2, 1, 50, Some(1), "ROS", 2, 0, 1, 0, 1)
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
    assertDFs(result, expected)
  }

  "makeOutcomes" should "create a single-column dataframe with the sparse time-dependent outcomes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 1),
      bucketSize = 3 // bucketCount = 10
    )
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, Some(6), "ROS", 2, 0, 6, 0, 1),
      LaggedExposure("PA", 0, 1, 75, Some(6), "PIO", 1, 1, 6, 0, 1),
      LaggedExposure("PA", 0, 1, 75, Some(6), "ROS", 2, 2, 6, 0, 1),
      LaggedExposure("PC", 2, 1, 50, Some(1), "ROS", 2, 0, 1, 0, 1)
    ).toDS()

    val expected = Seq(6, 21).toDS.toDF

    // When
    val writer = MLPPWriter(params)
    import writer.DiscreteExposures
    val result = input.makeOutcomes

    // Then
    assertDFs(result, expected)
  }

  "makeStaticOutcomes" should "create a single-column dataframe with the sparse static outcomes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 1),
      bucketSize = 3 // bucketCount = 10
    )
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, Some(6), "ROS", 2, 0, 6, 0, 1),
      LaggedExposure("PA", 0, 1, 75, Some(6), "PIO", 1, 1, 6, 0, 1),
      LaggedExposure("PA", 0, 1, 75, Some(6), "ROS", 2, 2, 6, 0, 1),
      LaggedExposure("PC", 2, 1, 50, Some(1), "ROS", 2, 0, 1, 0, 1)
    ).toDS()

    val expected = Seq(0, 2).toDS.toDF

    // When
    val writer = MLPPWriter(params)
    import writer.DiscreteExposures
    val result = input.makeStaticOutcomes

    // Then
    assertDFs(result, expected)
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
      FlatEvent("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 4, 15)), "exposure", "Mol1", 1.0, makeTS(2006, 1, 15), None),
      FlatEvent("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 4, 15)), "exposure", "Mol1", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 4, 15)), "disease", "targetDisease", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 1, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 4, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol2", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol3", 1.0, makeTS(2006, 4, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "disease", "targetDisease", 1.0, makeTS(2006, 5, 15), None)
    ).toDS

    val expectedFeatures = Seq(
      // Patient A
      MLPPFeature("PA", 0, "Mol1", 0, 0, 0, 0,  0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 1, 1, 1,  1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 2, 2,  2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 3, 3,  3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 0, 2,  0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 1, 3,  1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 2, 4,  2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 3, 5,  3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 0, 3,  0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 1, 4,  1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 2, 5,  2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 6, 3, 6,  3, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 2, 0, 2,  4, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 3, 1, 3,  5, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 4, 2, 4,  6, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 5, 3, 5,  7, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 3, 0, 3,  8, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 4, 1, 4,  9, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 5, 2, 5, 10, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 6, 3, 6, 11, 1.0),
      // Patient B
      MLPPFeature("PB", 1, "Mol1", 0, 0, 0,  7,  0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 1, 1,  8,  1, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 2, 2,  9,  2, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 2, 0,  9,  0, 1.0)
    ).toDF

    val expectedZMatrix = Seq(
      (3D, 1D, 1D, 46, 1, "PA", 0),
      (2D, 0D, 0D, 56, 1, "PB", 1),
      (1D, 0D, 0D, 36, 2, "PC", 2)
    ).toDF("MOL0000_Mol1", "MOL0001_Mol2", "MOL0002_Mol3", "age", "gender", "patientID", "patientIDIndex")

    // When
    val result = MLPPWriter(params).write(input, rootDir).toDF
    val writtenResult = sqlContext.read.parquet(s"$rootDir/parquet/SparseFeatures")
    val StaticExposures = sqlContext.read.parquet(s"$rootDir/parquet/StaticExposures")

    // Then
    assertDFs(result, expectedFeatures)
   assertDFs(writtenResult, expectedFeatures)
   assertDFs(StaticExposures, expectedZMatrix)
 }


  it should "create the final matrices and write them as parquet files (removing death bucket)" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val rootDir = "target/test/output"
    val params = MLPPWriter.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 8, 1), // 7 total buckets
      bucketSize = 30,
      lagCount = 4,
      includeDeathBucket = true
    )
    val input: Dataset[FlatEvent] = Seq(
      FlatEvent("PC", 2, makeTS(1970, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 5, 15), None),
      FlatEvent("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 4, 15)), "exposure", "Mol1", 1.0, makeTS(2006, 1, 15), None),
      FlatEvent("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 4, 15)), "exposure", "Mol1", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 4, 15)), "disease", "targetDisease", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 1, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol1", 1.0, makeTS(2006, 4, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol2", 1.0, makeTS(2006, 3, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "exposure", "Mol3", 1.0, makeTS(2006, 4, 15), None),
      FlatEvent("PA", 1, makeTS(1960, 1, 1), None, "disease", "targetDisease", 1.0, makeTS(2006, 5, 15), None)
    ).toDS

    val expectedFeatures = Seq(
      // Patient A
      MLPPFeature("PA", 0, "Mol1", 0, 0, 0, 0,  0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 1, 1, 1,  1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 2, 2,  2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 3, 3,  3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 0, 2,  0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 1, 3,  1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 2, 4,  2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 3, 5,  3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 0, 3,  0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 1, 4,  1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 2, 5,  2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 6, 3, 6,  3, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 2, 0, 2,  4, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 3, 1, 3,  5, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 4, 2, 4,  6, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 5, 3, 5,  7, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 3, 0, 3,  8, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 4, 1, 4,  9, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 5, 2, 5, 10, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 6, 3, 6, 11, 1.0),
      // Patient A,
      MLPPFeature("PB", 1, "Mol1", 0, 0, 0,  7,  0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 1, 1,  8,  1, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 2, 2,  9,  2, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 3, 3, 10,  3, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 2, 0,  9,  0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 3, 1, 10,  1, 1.0)
    ).toDF

    val expectedZMatrix = Seq(
      (3D, 1D, 1D, 46, 1, "PA", 0),
      (2D, 0D, 0D, 56, 1, "PB", 1),
      (1D, 0D, 0D, 36, 2, "PC", 2)
    ).toDF("MOL0000_Mol1", "MOL0001_Mol2", "MOL0002_Mol3", "age", "gender", "patientID", "patientIDIndex")

    // When
    val result = MLPPWriter(params).write(input, rootDir).toDF
    val writtenResult = sqlContext.read.parquet(s"$rootDir/parquet/SparseFeatures")
    val StaticExposures = sqlContext.read.parquet(s"$rootDir/parquet/StaticExposures")

    // Then
    assertDFs(result, expectedFeatures)
   assertDFs(writtenResult, expectedFeatures)
   assertDFs(StaticExposures, expectedZMatrix)
 }
}
