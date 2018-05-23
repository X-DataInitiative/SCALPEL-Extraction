package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import fr.polytechnique.cmap.cnam.SharedContext


class MLPPMainSuite extends SharedContext{

  "getEnv" should "return correct default environment" in {
    // Given
    val args = Map[String, String]()
    val expected = MLPPMain.TestEnv

    // When
    val result = MLPPMain.getEnv(args)

    // Then
    assert(result == expected)
  }

  it should "return the Fall environment when specified" in {
    // Given
    val args = Map(("env", "fall"))
    val expected = MLPPMain.FallEnv

    // When
    val result = MLPPMain.getEnv(args)

    // Then
    assert(result == expected)
  }

  it should "return the CMAP environment when specified" in {

    // Given
    val args = Map(("env", "cmap"))
    val expected = MLPPMain.CmapEnv

    // When
    val result = MLPPMain.getEnv(args)

    // Then
    assert(result == expected)
  }
  
  "run" should "run the whole pipeline with MLPP featuring" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val expectedFeatures = Seq(
      // Patient A
      MLPPFeature("PA", 0, "Mol1", 0, 14, 0, 14,  0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 73, 0, 73,  0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 104, 0, 104,  0, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 73, 0, 73,  1, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 104, 0, 104,  2, 1.0),
      // Patient B
      MLPPFeature("PB", 1, "Mol1", 0, 14, 0,  226,  0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 73, 0,  285,  0, 1.0)
    ).toDF("patientID", "patientIndex", "exposureType", "exposureTypeIndex", "bucketIndex", "lagIndex", "rowIndex", "colIndex", "value")

    // When
    val configPath = "src/test/resources/config/filtering-broad.conf"
    MLPPMain.run(sqlContext, Map("conf" -> configPath, "env" -> "test", "study" -> "pioglitazone"))

    // Then
    val mlppSparseFeatures = sqlCtx.read.parquet(MLPPMain.TestEnv.featuringPath + "/parquet/SparseFeatures")

    assertDFs(mlppSparseFeatures, expectedFeatures)

  }

}
