package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.{daysBetween, makeTS}

class DiscreteExposureImplicitsSuite extends SharedContext {

  "makeMetadata" should "create a metadata dataset with some relevant information" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 1),
      bucketSize = 2,
      lagCount = 4
    )
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol3", 2, 1, 6, 1, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol2", 1, 2, 6, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol3", 2, 1, 8, 1, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol4", 3, 6, 8, 2, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol5", 4, 7, 8, 3, 1.0)
    ).toDS
    val discreteExposureImplicits = new DiscreteExposureImplicits(input)

    val expected: Metadata = Metadata(30, 20, 2, 15, 2, 5, 4)

    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
    // When
    val result = discreteExposureImplicits.makeMetadata(params.lagCount, bucketCount, params.bucketSize)

    // Then
    assert(result == expected)
  }

  "lagExposures" should "create the lagged elements of the matrix for each exposure" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(lagCount = 4)
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol2", 1, 4, 6, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 4, 8, 0, 1.0)
    ).toDS
    val discreteExposureImplicits = new DiscreteExposureImplicits(input)

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
    val result = discreteExposureImplicits.lagExposures(params.lagCount).toDF

    // Then
    assertDFs(result, expected)
  }

  "toMLPPFeatures" should "create a new Dataset with the final COO sparse matrix format" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
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
    val discreteExposureImplicits = new DiscreteExposureImplicits(input)

    val expected = Seq(
      // pId, pIdx, mol, molIdx, bkt, lag, row, col, val
      MLPPFeature("PA", 0, "Mol1", 0, 0, 0, 0, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 1, 1, 1, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 2, 2, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 3, 3, 3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 0, 2, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 1, 3, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 2, 4, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 3, 5, 3, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 4, 0, 4, 4, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 5, 1, 5, 5, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 0, 0, 16, 0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 1, 1, 17, 1, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 2, 2, 18, 2, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 3, 3, 19, 3, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 4, 0, 20, 0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 5, 1, 21, 1, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 6, 2, 22, 2, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 7, 3, 23, 3, 1.0)
    ).toDF
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
    // When
    val result = discreteExposureImplicits.toMLPPFeatures(params.lagCount, bucketCount).toDF

    // Then
    assertDFs(result, expected)
  }

  "makeStaticExposures" should "compute the static exposures matrix in a DataFrame format" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, "ROS", 2, 0, 6, 0, 1),
      LaggedExposure("PA", 0, 1, 75, "PIO", 1, 1, 6, 0, 1),
      LaggedExposure("PA", 0, 1, 75, "ROS", 2, 2, 6, 0, 1),
      LaggedExposure("PB", 1, 2, 40, "INS", 0, 1, 8, 0, 1),
      LaggedExposure("PC", 2, 1, 50, "ROS", 2, 0, 1, 0, 1)
    ).toDS()
    val discreteExposureImplicits = new DiscreteExposureImplicits(input)

    val expected = Seq(
      (0D, 1D, 2D, 75, 1, "PA", 0),
      (1D, 0D, 0D, 40, 2, "PB", 1),
      (0D, 0D, 1D, 50, 1, "PC", 2)
    ).toDF("MOL0000_INS", "MOL0001_PIO", "MOL0002_ROS", "age", "gender", "patientID", "patientIDIndex")

    // When
    val result = discreteExposureImplicits.makeStaticExposures

    // Then
    assertDFs(result, expected)
  }

  "makeCensoring" should "create the output dataframe with censoring information" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 1),
      bucketSize = 3 // bucketCount = 10
    )
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, "ROS", 2, 0, 5, 0, 1),
      LaggedExposure("PA", 0, 1, 75, "PIO", 1, 1, 5, 0, 1),
      LaggedExposure("PA", 0, 1, 75, "ROS", 2, 2, 5, 0, 1),
      LaggedExposure("PC", 2, 1, 65, "PIO", 1, 1, 7, 0, 1),
      LaggedExposure("PC", 2, 1, 65, "ROS", 2, 2, 7, 0, 1),
      LaggedExposure("PD", 3, 1, 50, "ROS", 2, 0, 1, 0, 1)
    ).toDS()
    val discreteExposureImplicits = new DiscreteExposureImplicits(input)

    val expected = Seq(5, 27, 31).toDS.toDF("index")
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
    // When
    val result = discreteExposureImplicits.makeCensoring(bucketCount, params.featuresAsList)

    // Then
    assertDFs(result, expected)
  }

  it should "create in the new format when featuresAsList is true" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(featuresAsList = true)
    val input: Dataset[LaggedExposure] = Seq(
      LaggedExposure("PA", 0, 1, 75, "ROS", 2, 0, 5, 0, 1),
      LaggedExposure("PA", 0, 1, 75, "PIO", 1, 1, 5, 0, 1),
      LaggedExposure("PA", 0, 1, 75, "ROS", 2, 2, 5, 0, 1),
      LaggedExposure("PC", 2, 1, 65, "PIO", 1, 1, 7, 0, 1),
      LaggedExposure("PC", 2, 1, 65, "ROS", 2, 2, 7, 0, 1),
      LaggedExposure("PD", 3, 1, 50, "ROS", 2, 0, 1, 0, 1)
    ).toDS()
    val discreteExposureImplicits = new DiscreteExposureImplicits(input)

    val expected = Seq(
      (0, 5),
      (2, 7),
      (3, 1)
    ).toDF("patientIndex", "bucket")
    val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
    // When
    val result = discreteExposureImplicits.makeCensoring(bucketCount, params.featuresAsList)

    // Then
    assertDFs(result, expected)
  }

}
