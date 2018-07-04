package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import java.nio.file.Paths
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config.MLPPConfig
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.functions._

class MLPPLoaderSuite extends SharedContext {

  "load" should "create the final matrices and write them as parquet files" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val tempPath = "target/test.conf"
    val strConf =
      """
        | input {
        |   patients: "src/test/resources/MLPP/patient"
        |   outcomes: "src/test/resources/MLPP/outcome"
        |   exposures: "src/test/resources/MLPP/exposure"
        | }
        |
        | output={
        |   root = "target/test/output/"
        | }
        |
        | base={
        |   lag_count = 4
        |   bucket_size = 30
        |   features_as_list = false
        | }
        |
        | extra={
        |   min_timestamp = "2006-01-01"
        |   max_timestamp = "2006-08-01"
        | }
      """.stripMargin
    pureconfig.saveConfigAsPropertyFile(ConfigFactory.parseString(strConf), Paths.get(tempPath), true)

    val params = MLPPConfig.load(tempPath, "test")

    val patient: Dataset[Patient] = Seq(
      Patient("PA", 1, makeTS(1960, 1, 1), None),
      Patient("PC", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 4, 15))),
      Patient("PB", 2, makeTS(1970, 1, 1), None)
    ).toDS

    val outcome: Dataset[Event[Outcome]] = Seq(
      Outcome("PA", "type1", "targetDisease", makeTS(2006, 5, 15)),
      Outcome("PC", "type2", "targetDisease", makeTS(2006, 3, 15))
    ).toDS

    val exposure: Dataset[Event[Exposure]] = Seq(
      Exposure("PB", "Mol1", 1.0, makeTS(2006, 5, 15), makeTS(1789, 12, 31)),
      Exposure("PC", "Mol1", 1.0, makeTS(2006, 1, 15), makeTS(1789, 12, 31)),
      Exposure("PC", "Mol1", 1.0, makeTS(2006, 3, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol1", 1.0, makeTS(2006, 1, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol1", 1.0, makeTS(2006, 3, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol1", 1.0, makeTS(2006, 4, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol2", 1.0, makeTS(2006, 3, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol3", 1.0, makeTS(2006, 4, 15), makeTS(1789, 12, 31))
    ).toDS

    val expectedFeatures = Seq(
      // Patient A
      MLPPFeature("PA", 0, "Mol1", 0, 0, 0, 0, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 1, 1, 1, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 2, 2, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 3, 3, 3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 0, 2, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 1, 3, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 2, 4, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 3, 5, 3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 0, 3, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 1, 4, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 2, 5, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 6, 3, 6, 3, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 2, 0, 2, 4, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 3, 1, 3, 5, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 4, 2, 4, 6, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 5, 3, 5, 7, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 3, 0, 3, 8, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 4, 1, 4, 9, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 5, 2, 5, 10, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 6, 3, 6, 11, 1.0),
      // Patient B
      MLPPFeature("PC", 1, "Mol1", 0, 0, 0, 7, 0, 1.0),
      MLPPFeature("PC", 1, "Mol1", 0, 1, 1, 8, 1, 1.0),
      MLPPFeature("PC", 1, "Mol1", 0, 2, 2, 9, 2, 1.0),
      MLPPFeature("PC", 1, "Mol1", 0, 2, 0, 9, 0, 1.0)
    ).toDF

    val expectedZMatrix = Seq(
      (3D, 1D, 1D, 46, 1, "PA", 0),
      (2D, 0D, 0D, 56, 1, "PC", 1)
    ).toDF("MOL0000_Mol1", "MOL0001_Mol2", "MOL0002_Mol3", "age", "gender", "patientID", "patientIDIndex")

    val expectedOutcomes: DataFrame = Seq(
      ("9", "1"),
      ("4", "0")
    ).toDF("patientBucketIndex", "diseaseTypeIndex")

    // When
    val result = MLPPLoader(params)
      .load(outcomes = outcome, exposures = exposure, patients = patient).toDF
    val writtenResult = sqlContext.read.parquet(params.output.sparseFeaturesParquet)
    val StaticExposures = sqlContext.read.parquet(params.output.staticExposuresParquet)
    val outcomesResult = sqlContext.read.option("header", true).csv(params.output.outcomes)

    // Then
    assertDFs(outcomesResult, expectedOutcomes)
    assertDFs(result, expectedFeatures)
    assertDFs(writtenResult, expectedFeatures)
    assertDFs(StaticExposures, expectedZMatrix)
  }

  it should "create the final matrices and write them as parquet files (removing death bucket)" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val tempPath = "target/test.conf"
    val strConf =
      """
        | input {
        |   patients: "src/test/resources/MLPP/patient"
        |   outcomes: "src/test/resources/MLPP/outcome"
        |   exposures: "src/test/resources/MLPP/exposure"
        | }
        |
        | output={
        |   root = "target/test/output/"
        | }
        |
        | base={
        |   lag_count = 4
        |   bucket_size = 30
        |   features_as_list = false
        | }
        |
        | extra={
        |   min_timestamp = "2006-01-01"
        |   max_timestamp = "2006-08-01"
        |   include_censored_bucket = true
        | }
      """.stripMargin
    pureconfig.saveConfigAsPropertyFile(ConfigFactory.parseString(strConf), Paths.get(tempPath), true)

    val params = MLPPConfig.load(tempPath, "test")

    val patient: Dataset[Patient] = Seq(
      Patient("PC", 2, makeTS(1970, 1, 1), None),
      Patient("PB", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 4, 15))),
      Patient("PA", 1, makeTS(1960, 1, 1), None)
    ).toDS

    val outcome: Dataset[Event[Outcome]] = Seq(
      Outcome("PB", "targetDisease", makeTS(2006, 3, 15)),
      Outcome("PA", "targetDisease", makeTS(2006, 5, 15))
    ).toDS

    val exposure: Dataset[Event[Exposure]] = Seq(
      Exposure("PC", "Mol1", 1.0, makeTS(2006, 5, 15), makeTS(1789, 12, 31)),
      Exposure("PB", "Mol1", 1.0, makeTS(2006, 1, 15), makeTS(1789, 12, 31)),
      Exposure("PB", "Mol1", 1.0, makeTS(2006, 3, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol1", 1.0, makeTS(2006, 1, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol1", 1.0, makeTS(2006, 3, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol1", 1.0, makeTS(2006, 4, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol2", 1.0, makeTS(2006, 3, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol3", 1.0, makeTS(2006, 4, 15), makeTS(1789, 12, 31))
    ).toDS

    val expectedFeatures = Seq(
      // Patient A
      MLPPFeature("PA", 0, "Mol1", 0, 0, 0, 0, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 1, 1, 1, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 2, 2, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 3, 3, 3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 0, 2, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 1, 3, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 2, 4, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 3, 5, 3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 0, 3, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 1, 4, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 2, 5, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 6, 3, 6, 3, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 2, 0, 2, 4, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 3, 1, 3, 5, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 4, 2, 4, 6, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 5, 3, 5, 7, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 3, 0, 3, 8, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 4, 1, 4, 9, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 5, 2, 5, 10, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 6, 3, 6, 11, 1.0),
      // Patient A,
      MLPPFeature("PB", 1, "Mol1", 0, 0, 0, 7, 0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 1, 1, 8, 1, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 2, 2, 9, 2, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 3, 3, 10, 3, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 2, 0, 9, 0, 1.0),
      MLPPFeature("PB", 1, "Mol1", 0, 3, 1, 10, 1, 1.0)
    ).toDF

    val expectedZMatrix = Seq(
      (3D, 1D, 1D, 46, 1, "PA", 0),
      (2D, 0D, 0D, 56, 1, "PB", 1)
    ).toDF("MOL0000_Mol1", "MOL0001_Mol2", "MOL0002_Mol3", "age", "gender", "patientID", "patientIDIndex")

    // When
    val result = MLPPLoader(params).load(outcomes = outcome, exposures = exposure, patients = patient).toDF
    val writtenResult = sqlContext.read.parquet(params.output.sparseFeaturesParquet)
    val StaticExposures = sqlContext.read.parquet(params.output.staticExposuresParquet)

    // Then
    assertDFs(result, expectedFeatures)
    assertDFs(writtenResult, expectedFeatures)
    assertDFs(StaticExposures, expectedZMatrix)
  }

  it should "create the final matrices and write them as parquet files with featuresAsList" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val tempPath = "target/test.conf"
    val strConf =
      """
        | input {
        |   patients: "src/test/resources/MLPP/patient"
        |   outcomes: "src/test/resources/MLPP/outcome"
        |   exposures: "src/test/resources/MLPP/exposure"
        | }
        |
        | output={
        |   root = "target/test/output/"
        | }
        |
        | base={
        |   lag_count = 4
        |   bucket_size = 30
        |   features_as_list = true
        | }
        |
        | extra={
        |   min_timestamp = "2006-01-01"
        |   max_timestamp = "2006-08-01"
        | }
      """.stripMargin
    pureconfig.saveConfigAsPropertyFile(ConfigFactory.parseString(strConf), Paths.get(tempPath), true)

    val params = MLPPConfig.load(tempPath, "test")

    val patient: Dataset[Patient] = Seq(
      Patient("PA", 1, makeTS(1960, 1, 1), None),
      Patient("PC", 1, makeTS(1950, 1, 1), Some(makeTS(2006, 4, 15))),
      Patient("PB", 2, makeTS(1970, 1, 1), None)
    ).toDS

    val outcome: Dataset[Event[Outcome]] = Seq(
      Outcome("PA", "type1", "targetDisease", makeTS(2006, 5, 15)),
      Outcome("PC", "type2", "targetDisease", makeTS(2006, 3, 15))
    ).toDS

    val exposure: Dataset[Event[Exposure]] = Seq(
      Exposure("PB", "Mol1", 1.0, makeTS(2006, 5, 15), makeTS(1789, 12, 31)),
      Exposure("PC", "Mol1", 1.0, makeTS(2006, 1, 15), makeTS(1789, 12, 31)),
      Exposure("PC", "Mol1", 1.0, makeTS(2006, 3, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol1", 1.0, makeTS(2006, 1, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol1", 1.0, makeTS(2006, 3, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol1", 1.0, makeTS(2006, 4, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol2", 1.0, makeTS(2006, 3, 15), makeTS(1789, 12, 31)),
      Exposure("PA", "Mol3", 1.0, makeTS(2006, 4, 15), makeTS(1789, 12, 31))
    ).toDS

    val expectedFeatures = Seq(
      // Patient A
      MLPPFeature("PA", 0, "Mol1", 0, 0, 0, 0, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 1, 1, 1, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 2, 2, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 3, 3, 3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 2, 0, 2, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 1, 3, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 2, 4, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 3, 5, 3, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 3, 0, 3, 0, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 4, 1, 4, 1, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 5, 2, 5, 2, 1.0),
      MLPPFeature("PA", 0, "Mol1", 0, 6, 3, 6, 3, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 2, 0, 2, 4, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 3, 1, 3, 5, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 4, 2, 4, 6, 1.0),
      MLPPFeature("PA", 0, "Mol2", 1, 5, 3, 5, 7, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 3, 0, 3, 8, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 4, 1, 4, 9, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 5, 2, 5, 10, 1.0),
      MLPPFeature("PA", 0, "Mol3", 2, 6, 3, 6, 11, 1.0),
      // Patient B
      MLPPFeature("PC", 1, "Mol1", 0, 0, 0, 7, 0, 1.0),
      MLPPFeature("PC", 1, "Mol1", 0, 1, 1, 8, 1, 1.0),
      MLPPFeature("PC", 1, "Mol1", 0, 2, 2, 9, 2, 1.0),
      MLPPFeature("PC", 1, "Mol1", 0, 2, 0, 9, 0, 1.0)
    ).toDF

    val expectedZMatrix = Seq(
      (3D, 1D, 1D, 46, 1, "PA", 0),
      (2D, 0D, 0D, 56, 1, "PC", 1)
    ).toDF("MOL0000_Mol1", "MOL0001_Mol2", "MOL0002_Mol3", "age", "gender", "patientID", "patientIDIndex")

    val expectedOutcomes: DataFrame = Seq(
      ("0", "4", "0"),
      ("1", "2", "1")
    ).toDF("patientIndex", "bucket", "diseaseType")


    // When
    val result = MLPPLoader(params)
      .load(outcomes = outcome, exposures = exposure, patients = patient).toDF
    val writtenResult = sqlContext.read.parquet(params.output.sparseFeaturesParquet)
    val StaticExposures = sqlContext.read.parquet(params.output.staticExposuresParquet)
    val outcomesResult = sqlContext.read.option("header", true).csv(params.output.outcomes)

    // Then
    assertDFs(outcomesResult, expectedOutcomes)
    assertDFs(result, expectedFeatures)
    assertDFs(writtenResult, expectedFeatures)
    assertDFs(StaticExposures, expectedZMatrix)
  }
}
