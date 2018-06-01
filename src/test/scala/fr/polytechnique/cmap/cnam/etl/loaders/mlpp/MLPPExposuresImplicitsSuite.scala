package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MLPPExposuresImplicitsSuite extends SharedContext {

  "makeDiscreteExposures" should "return a Dataset containing the 0-lag exposures in the sparse format" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val params = MLPPLoader.Params(
      minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 2),
      bucketSize = 2
    )

    val input = Seq(
      ("PA", 0, "exposure", 1, 75, Some(6), "Mol1", 0, 0, 6),
      ("PA", 0, "exposure", 1, 75, Some(6), "Mol1", 0, 0, 6),
      ("PA", 0, "exposure", 1, 75, Some(6), "Mol1", 0, 2, 6),
      ("PA", 0, "exposure", 1, 75, Some(6), "Mol2", 1, 3, 6),
      ("PB", 1, "exposure", 2, 40, None, "Mol1", 0, 0, 8),
      ("PB", 1, "exposure", 2, 40, None, "Mol1", 0, 4, 8),
      ("PB", 1, "exposure", 2, 40, None, "Mol1", 0, 4, 8),
      ("PB", 1, "exposure", 2, 40, None, "Mol2", 1, 6, 8)
    ).toDF("patientID", "patientIDIndex", "category", "gender", "age", "diseaseBucket", "exposureType", "exposureTypeIndex", "startBucket", "endBucket")

    val expected = Seq(
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 0, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol1", 0, 2, 6, 0, 1.0),
      LaggedExposure("PA", 0, 1, 75, "Mol2", 1, 3, 6, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 0, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol1", 0, 4, 8, 0, 1.0),
      LaggedExposure("PB", 1, 2, 40, "Mol2", 1, 6, 8, 0, 1.0)
    ).toDF

    // When
    val result = new MLPPExposures(params).makeExposures(input).select(expected.columns.map(col): _*)

    // Then
    assertDFs(result, expected)
  }

}
