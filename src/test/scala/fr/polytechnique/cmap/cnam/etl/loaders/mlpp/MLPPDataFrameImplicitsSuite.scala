package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.{daysBetween, makeTS}

class MLPPDataFrameImplicitsSuite extends SharedContext {

  "makeMLPPDataFrame" should "build a MLPP data frame with implicit functions" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2
    )

    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(1950, 1, 1),
        makeTS(2006, 1, 1), Some(makeTS(2006, 1, 30))),
      ("Patient_A", "trackloss", "trackloss", makeTS(1950, 1, 1),
        makeTS(2006, 1, 10), Some(makeTS(2006, 1, 30))),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(1950, 1, 1),
        makeTS(2006, 1, 1), Some(makeTS(2006, 1, 30))),
      ("Patient_B", "trackloss", "trackloss", makeTS(1950, 1, 1),
        makeTS(2006, 1, 10), Some(makeTS(2006, 1, 30)))
    ).toDF("patientID", "category", "eventId", "birthDate", "start", "deathDate")

    val expected = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(1950, 1, 1),
        makeTS(2006, 1, 1), Some(makeTS(2006, 1, 30)),
        56, 0, Some(14), Some(4), Some(4)),
      ("Patient_A", "trackloss", "trackloss", makeTS(1950, 1, 1),
        makeTS(2006, 1, 10), Some(makeTS(2006, 1, 30)),
        56, 4, Some(14), Some(4), Some(4)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(1950, 1, 1),
        makeTS(2006, 1, 1), Some(makeTS(2006, 1, 30)),
        56, 0, Some(14), Some(4), Some(4)),
      ("Patient_B", "trackloss", "trackloss", makeTS(1950, 1, 1),
        makeTS(2006, 1, 10), Some(makeTS(2006, 1, 30)),
        56, 4, Some(14), Some(4), Some(4))
    ).toDF("patientID", "category", "eventId", "birthDate", "start", "deathDate", "age", "startBucket",
      "deathBucket", "tracklossBucket", "endBucket")

    // When
    val result = new MLPPDataFrame(params).makeMLPPDataFrame(input)

    // Then
    assertDFs(result, expected)

  }

  "withAge" should "add a column with the patients age at the reference date" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val params = MLPPLoader.Params()

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
    val mlppDataFrameImplicits = new MLPPDataFrameImplicits(input)

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
    val result = mlppDataFrameImplicits.withAge(params.minTimestamp)

    // Then
    assertDFs(result, expected)
  }

  "withStartBucket" should "add a column with the start date bucket of the event" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2
    )

    val input = Seq(
      ("PA", makeTS(2006, 1, 1)),
      ("PB", makeTS(2006, 1, 5)),
      ("PC", makeTS(2006, 1, 10))
    ).toDF("patientID", "start")
    val mlppDataFrameImplicits = new MLPPDataFrameImplicits(input)

    val expected = Seq(
      ("PA", makeTS(2006, 1, 1), 0),
      ("PB", makeTS(2006, 1, 5), 2),
      ("PC", makeTS(2006, 1, 10), 4)
    ).toDF("patientID", "start", "startBucket")

    // When
    val result = mlppDataFrameImplicits.withStartBucket(params.minTimestamp, params.maxTimestamp, params.bucketSize)

    // Then
    assertDFs(result, expected)
  }

  "withDeathBucket" should "add a column with the death date bucket of the patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2
    )

    val input = Seq(
      ("PA", Some(makeTS(2006, 1, 1))),
      ("PA", None),
      ("PB", Some(makeTS(2006, 1, 5))),
      ("PC", Some(makeTS(2006, 1, 10)))
    ).toDF("patientID", "deathDate")
    val mlppDataFrameImplicits = new MLPPDataFrameImplicits(input)

    val expected = Seq(
      ("PA", Some(makeTS(2006, 1, 1)), Some(0)),
      ("PA", None, None),
      ("PB", Some(makeTS(2006, 1, 5)), Some(2)),
      ("PC", Some(makeTS(2006, 1, 10)), Some(4))
    ).toDF("patientID", "deathDate", "deathBucket")

    // When
    val result = mlppDataFrameImplicits.withDeathBucket(params.minTimestamp, params.maxTimestamp, params.bucketSize)

    // Then
    assertDFs(result, expected)
  }

  "withTracklossBucket" should "add a column with the timeBucket of the first trackloss of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val params = MLPPLoader.Params()

    // Given
    val input = Seq(
      ("PA", "molecule", "PIOGLITAZONE", 0, Some(4)),
      ("PA", "molecule", "PIOGLITAZONE", 5, Some(4)),
      ("PA", "trackloss", "trackloss", 3, Some(4)),
      ("PB", "molecule", "PIOGLITAZONE", 2, None),
      ("PB", "trackloss", "trackloss", 4, None),
      ("PC", "molecule", "PIOGLITAZONE", 0, Some(6)),
      ("PD", "molecule", "PIOGLITAZONE", 2, Some(3)),
      ("PD", "molecule", "PIOGLITAZONE", 3, Some(3)),
      ("PD", "trackloss", "trackloss", 4, Some(3))
    ).toDF("patientID", "category", "eventId", "startBucket", "deathBucket")
    val mlppDataFrameImplicits = new MLPPDataFrameImplicits(input)

    val expected = Seq(
      ("PA", "molecule", "PIOGLITAZONE", 0, Some(4), Some(3)),
      ("PA", "molecule", "PIOGLITAZONE", 5, Some(4), Some(3)),
      ("PA", "trackloss", "trackloss", 3, Some(4), Some(3)),
      ("PB", "molecule", "PIOGLITAZONE", 2, None, Some(4)),
      ("PB", "trackloss", "trackloss", 4, None, Some(4)),
      ("PC", "molecule", "PIOGLITAZONE", 0, Some(6), None),
      ("PD", "molecule", "PIOGLITAZONE", 2, Some(3), None),
      ("PD", "molecule", "PIOGLITAZONE", 3, Some(3), None),
      ("PD", "trackloss", "trackloss", 4, Some(3), None)
    ).toDF("patientID", "category", "eventId", "startBucket", "deathBucket", "tracklossBucket")

    // When
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
    val result = mlppDataFrameImplicits.withTracklossBucket(bucketCount)

    // Then
    assertDFs(result, expected)
  }

  "withEndBucket" should "add a column with the minimum among deathBucket and the max number of buckets" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2
    )

    val input = Seq(
      ("PA", Some(2), None),
      ("PB", Some(4), Some(5)),
      ("PC", None, Some(5)),
      ("PD", Some(5), None),
      ("PE", Some(7), Some(6)),
      ("PF", None, None)
    ).toDF("patientID", "deathBucket", "tracklossBucket")
    val mlppDataFrameImplicits = new MLPPDataFrameImplicits(input)

    val expected = Seq(
      ("PA", 2),
      ("PB", 4),
      ("PC", 5),
      ("PD", 5),
      ("PE", 6),
      ("PF", 16)
    ).toDF("patientID", "endBucket")

    // When
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
    val result = mlppDataFrameImplicits.withEndBucket(params.includeCensoredBucket, bucketCount)
      .select("patientID", "endBucket")

    // Then
    assertDFs(result, expected)
  }

  it should "add a column with the minimum among deathBucket + 1, and the max number of buckets if " +
    "includeDeathBucket is true" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2,
      includeCensoredBucket = true
    )

    val input = Seq(
      ("PA", Some(16), None),
      ("PA", Some(16), None),
      ("PB", Some(0), Some(2)),
      ("PB", Some(0), Some(2)),
      ("PC", Some(5), Some(3)),
      ("PC", Some(5), Some(3)),
      ("PD", None, None),
      ("PD", None, None)
    ).toDF("patientID", "deathBucket", "tracklossBucket")
    val mlppDataFrameImplicits = new MLPPDataFrameImplicits(input)

    val expected = Seq(
      ("PA", Some(16)),
      ("PA", Some(16)),
      ("PB", Some(1)),
      ("PB", Some(1)),
      ("PC", Some(4)),
      ("PC", Some(4)),
      ("PD", Some(16)),
      ("PD", Some(16))
    ).toDF("patientID", "endBucket")

    // When
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
    val result = mlppDataFrameImplicits.withEndBucket(params.includeCensoredBucket, bucketCount)
      .select("patientID", "endBucket")

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
    val mlppDataFrameImplicits = new MLPPDataFrameImplicits(input)

    val expected = Seq(
      ("PA", "Mol1", 0, 0),
      ("PA", "Mol2", 0, 1),
      ("PA", "Mol2", 0, 1),
      ("PB", "Mol1", 1, 0),
      ("PB", "Mol1", 1, 0),
      ("PC", "Mol3", 2, 2)
    ).toDF("patientID", "molecule", "patientIDIndex", "moleculeIndex")

    // When
    val result = mlppDataFrameImplicits.withIndices(Seq("patientID", "molecule")).toDF

    // Then
    assertDFs(result, expected)
  }

  it should "still work with larger data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val rdd = sc.parallelize(for (i <- 0 until 50000; j <- 1 to 10) yield (i, i % 1000))
    val input = rdd.map(i => (f"${i._1}%015d", f"${i._2}%03d")).toDF("pID", "mol")
    val mlppDataFrameImplicits = new MLPPDataFrameImplicits(input)
    val expected = rdd.map(i => (i._1, i._2)).toDF("pIDIndex", "molIndex")

    // When
    val result = mlppDataFrameImplicits.withIndices(Seq("pID", "mol")).select("pIDIndex", "molIndex")

    // Then
    assertDFs(result, expected)
  }

}
