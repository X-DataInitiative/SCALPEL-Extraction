package fr.polytechnique.cmap.cnam.filtering

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Dataset
import org.scalatest.BeforeAndAfter
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames

class CoxFeaturesWriterSuite extends SharedContext with BeforeAndAfter{

  override def beforeEach(): Unit ={
    val directory = new File("anyPath")
    FileUtils.deleteDirectory(directory)
    super.afterEach()
  }

  "writeCSV" should "write a CSV file with cox features" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val features: Dataset[CoxFeature] = Seq(
      CoxFeature("Patient_A", 1, 678, 19, 30, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0),
      CoxFeature("Patient_A", 1, 678, 4, 19, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0),
      CoxFeature("Patient_B", 1, 792, 1, 26, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0)
    ).toDS
    val path = "anyPath/coxFeatures.csv"
    val expectedCount = 3

    // When
    import CoxFeaturesWriter.CoxFeatures
    features.writeCSV(path)
    val result = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      .as[CoxFeature]

    // Then
    result.printSchema
    features.printSchema
    result.show
    features.show
    assert(result.count == expectedCount)
  }
}
