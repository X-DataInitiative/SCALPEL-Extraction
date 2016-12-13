package fr.polytechnique.cmap.cnam.filtering.mlpp

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.FilteringConfig

class MLPPMainSuite extends SharedContext {

  override def beforeEach(): Unit = {
    super.beforeEach()
    val c = FilteringConfig.getClass.getDeclaredConstructor()
    c.setAccessible(true)
    c.newInstance()
    val c2 = MLPPConfig.getClass.getDeclaredConstructor()
    c2.setAccessible(true)
    c2.newInstance()
  }

  "run" should "correctly run the full mlpp featuring pipeline with default parameters" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/mlpp-default.conf"
    lazy val featuresPath = FilteringConfig.outputPaths.mlppFeatures

    val expectedFeatures: DataFrame = Seq(
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  0, 0,  0, 0, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  1, 0,  1, 0, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  1, 1,  1, 1, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  2, 1,  2, 1, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  2, 2,  2, 2, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  3, 2,  3, 2, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  3, 3,  3, 3, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  4, 3,  4, 3, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  4, 4,  4, 4, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  5, 4,  5, 4, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  5, 5,  5, 5, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  6, 5,  6, 5, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  6, 6,  6, 6, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  7, 6,  7, 6, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  7, 7,  7, 7, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  8, 7,  8, 7, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  8, 8,  8, 8, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  9, 8,  9, 8, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0,  9, 9,  9, 9, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 10, 9, 10, 9, 1.0)
    ).toDF

    // When
    val features: Dataset[MLPPFeature] = MLPPMain.run(sqlContext, Map("conf" -> configPath)).get
    val result = features.toDF

    // Then
    import RichDataFrames._
    result.orderBy("rowIndex", "colIndex").show
    expectedFeatures.orderBy("rowIndex", "colIndex").show
    assert(result === expectedFeatures)
  }


  "run" should "correctly run the full mlpp featuring pipeline with a new exposure definition" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/mlpp-new-exposure.conf"
    lazy val featuresPath = FilteringConfig.outputPaths.mlppFeatures

    val expectedFeatures: DataFrame = Seq(
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 0, 0, 0, 0, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 1, 1, 1, 1, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 2, 2, 2, 2, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 3, 3, 3, 3, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 4, 4, 4, 4, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 5, 5, 5, 5, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 6, 6, 6, 6, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 7, 7, 7, 7, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 8, 8, 8, 8, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 9, 9, 9, 9, 1.0)
    ).toDF

    // When
    val features: Dataset[MLPPFeature] = MLPPMain.run(sqlContext, Map("conf" -> configPath)).get
    val result = features.toDF

    // Then
    import RichDataFrames._
    result.orderBy("rowIndex", "colIndex").show
    expectedFeatures.orderBy("rowIndex", "colIndex").show
    assert(result === expectedFeatures)
  }
}
