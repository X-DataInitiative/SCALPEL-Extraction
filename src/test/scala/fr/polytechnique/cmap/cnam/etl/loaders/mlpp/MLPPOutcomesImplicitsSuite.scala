package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.{daysBetween, makeTS}

class MLPPOutcomesImplicitsSuite extends SharedContext {

  "makeOutcomes" should "build an outcome with implicit functions" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 1),
      bucketSize = 3, // bucketCount = 10
      featuresAsList = true
    )

    val input = Seq(
      ("PA", 0, "outcome", "femur", 3, Some(4)),
      ("PA", 0, "outcome", "main", 5, Some(6)),
      ("PB", 1, "outcome", "femur", 4, Some(16))
    ).toDF("patientID", "patientIDIndex", "category", "groupID", "startBucket", "endBucket")

    val expected1 = Seq(
      (0, 3, 0),
      (0, 5, 1),
      (1, 4, 0)
    ).toDF("patientIndex", "bucket", "diseaseType")

    val expected2 = Seq(0, 1).toDF("patientIDIndex")

    // When
    val mlppOutcomes = new MLPPOutcomes(params)
    val result = mlppOutcomes.makeOutcomes(input)
    import mlppOutcomes._
    val result1 = result.makeOutcomes(params.featuresAsList, 10)
    val result2 = result.makeStaticOutcomes
    //then
    assertDFs(result1, expected1)
    assertDFs(result2, expected2)
  }

  "withDiseaseBucket" should "add a column with the timeBucket of the first targetDisease of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val params = MLPPLoader.Params()
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
    // Given
    val input = Seq(
      ("PA", "outcome", "targetDisease", "type1", 3, Some(4)),
      ("PA", "outcome", "targetDisease", "type1", 5, Some(6)),
      ("PB", "outcome", "targetDisease", "type2", 4, Some(16)),
      ("PD", "outcome", "targetDisease", "type1", 4, Some(3))
    ).toDF("patientID", "category", "value", "diseaseType", "startBucket", "endBucket")
    val mlppOutcomesImplicits = new MLPPOutcomesImplicits(input)

    val expected = Seq(
      ("PA", "outcome", "targetDisease", "type1", 3, Some(4), Some(3)),
      ("PA", "outcome", "targetDisease", "type1", 5, Some(6), Some(3)),
      ("PB", "outcome", "targetDisease", "type2", 4, Some(16), Some(4)),
      ("PD", "outcome", "targetDisease", "type1", 4, Some(3), None)
    ).toDF("patientID", "category", "value", "diseaseType", "startBucket", "endBucket", "diseaseBucket")

    // When
    val result = mlppOutcomesImplicits.withDiseaseBucket(true, bucketCount)

    // Then
    assertDFs(result, expected)
  }

  "withDiseaseBucket" should "add a column with the timeBucket of the targetDisease of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val params = MLPPLoader.Params()
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
    // Given
    val input = Seq(
      ("PA", "outcome", "targetDisease", 3, Some(4)),
      ("PA", "outcome", "targetDisease", 5, Some(6)),
      ("PB", "outcome", "targetDisease", 4, Some(16)),
      ("PD", "outcome", "targetDisease", 4, Some(3))
    ).toDF("patientID", "category", "value", "startBucket", "endBucket")
    val mlppOutcomesImplicits = new MLPPOutcomesImplicits(input)

    val expected = Seq(
      ("PA", "outcome", "targetDisease", 3, Some(4), Some(3)),
      ("PA", "outcome", "targetDisease", 5, Some(6), Some(5)),
      ("PB", "outcome", "targetDisease", 4, Some(16), Some(4)),
      ("PD", "outcome", "targetDisease", 4, Some(3), None)
    ).toDF("patientID", "category", "value", "startBucket", "endBucket", "diseaseBucket")

    // When
    val result = mlppOutcomesImplicits.withDiseaseBucket(false, bucketCount)

    // Then
    assertDFs(result, expected)
  }

  "makeOutcomes" should "create a double-column dataframe with the sparse time-dependent outcomes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 1),
      bucketSize = 3 // bucketCount = 10
    )
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt

    val input: DataFrame = Seq(
      ("PA", 0, 1, 75, Some(6), "femur", 0, 0, 6),
      ("PA", 0, 1, 75, Some(6), "main", 1, 2, 6),
      ("PC", 2, 1, 50, Some(1), "femur", 0, 0, 1)
    ).toDF("patientID", "patientIDIndex", "gender", "age", "diseaseBucket", "diseaseType", "diseaseTypeIndex", "startBucket", "endBucket")
    val mlppOutcomesImplicits = new MLPPOutcomesImplicits(input)

    val expected = Seq(
      (6, 0),
      (6, 1),
      (21, 0)
    ).toDS.toDF("patientBucketIndex", "diseaseTypeIndex")

    // When
    val result = mlppOutcomesImplicits.makeOutcomes(params.featuresAsList, bucketCount)
    // Then
    assertDFs(result, expected)
  }

  it should "create in the new format when featuresAsList is true" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(featuresAsList = true)
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt

    val input: DataFrame = Seq(
      ("PA", 0, 1, 75, Some(6), "femur", 2, 0, 6),
      ("PA", 0, 1, 75, Some(6), "main", 1, 1, 6),
      ("PA", 0, 1, 75, Some(6), "femur", 2, 2, 6),
      ("PC", 2, 1, 50, Some(1), "femur", 2, 0, 1)
    ).toDF("patientID", "patientIDIndex", "gender", "age", "diseaseBucket", "diseaseType", "diseaseTypeIndex", "startBucket", "endBucket")
    val mlppOutcomesImplicits = new MLPPOutcomesImplicits(input)

    val expected = Seq(
      (0, 6, 1),
      (0, 6, 2),
      (2, 1, 2)
    ).toDF("patientIndex", "bucket", "diseaseType")

    // When

    val result = mlppOutcomesImplicits.makeOutcomes(params.featuresAsList, bucketCount)

    // Then
    assertDFs(result, expected)
  }

  "makeStaticOutcomes" should "create a single-column dataframe with the sparse static outcomes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 1),
      bucketSize = 3 // bucketCount = 10
    )
    val input: DataFrame = Seq(
      ("PA", 0, 1, 75, Some(6), "femur", 2, 0, 6),
      ("PA", 0, 1, 75, Some(6), "main", 1, 1, 6),
      ("PA", 0, 1, 75, Some(6), "femur", 2, 2, 6),
      ("PC", 2, 1, 50, Some(1), "femur", 2, 0, 1)
    ).toDF("patientID", "patientIDIndex", "gender", "age", "diseaseBucket", "diseaseType", "diseaseTypeIndex", "startBucket", "endBucket")
    val mlppOutcomesImplicits = new MLPPOutcomesImplicits(input)

    val expected = Seq(2, 0).toDS.toDF("patientIDIndex")

    // When
    val result = mlppOutcomesImplicits.makeStaticOutcomes

    // Then
    assertDFs(result, expected)
  }

}
