package fr.polytechnique.cmap.cnam.util.reporting

import java.io.File
import scala.io.Source
import org.apache.spark.sql.{AnalysisException, SQLContext}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.Path

class OperationReporterSuite extends SharedContext {

  lazy val sqlCtx: SQLContext = super.sqlContext

  "report" should "Return the correct metadata" in {
    import sqlCtx.implicits._

    // Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = Path("target/test/output")
    val expected = OperationMetadata(
      "test", List("input"),
      OperationTypes.AnyEvents,
      Some(Path(path, "test", "data").toString),
      Some(Path(path, "test", "patients").toString)
    )

    // When
    val result: OperationMetadata = OperationReporter
      .report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path)

    // Then
    assert(result == expected)
  }

  it should "Write the data correctly" in {
    import sqlCtx.implicits._

    // Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = Path("target/test/output")
    val pathWithTimestamp = Path("target/test/dummy/output")
    val expected = data
    val expectedAppend = data.union(data)

    // When
    var meta = OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path, "overwrite")
    val result = spark.read.parquet(meta.outputPath.get)
    val exception = intercept[Exception] {
      OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path)
    }
    meta = OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path, "append")
    val resultAppend = spark.read.parquet(meta.outputPath.get)
    meta = OperationReporter
      .report("test", List("input"), OperationTypes.AnyEvents, data.toDF, pathWithTimestamp, "withTimestamp")
    val resultWithTimestamp = spark.read.parquet(meta.outputPath.get)


    // Then
    assertDFs(result, expected)
    assert(exception.isInstanceOf[AnalysisException])
    assertDFs(resultAppend, expectedAppend)
    assertDFs(resultWithTimestamp, expected)

  }

  it should "Write the patient ids correctly" in {
    import sqlCtx.implicits._

    // Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = Path("target/test/output")
    val expected = Seq("Patient_A", "Patient_B").toDF("patientID")

    // When
    OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path)
    val result = spark.read.parquet(Path(path, "test", "patients").toString)

    // Then
    assertDFs(result, expected)
  }

  it should "Write only output (with all columns) for operation of OperationType.Patients" in {
    import sqlCtx.implicits._

    // Given
    val data = Seq(("Patient_A", 1), ("Patient_B", 2)).toDF("patientID", "other_col")
    val basePath = Path("target/test/output")
    val expectedMetadata = OperationMetadata(
      "test", List("input"),
      OperationTypes.Patients,
      Some(Path(basePath, "test", "data").toString),
      None
    )
    val expectedData = Seq(("Patient_A", 1), ("Patient_B", 2)).toDF("patientID", "other_col")

    // When
    val resultMetadata: OperationMetadata = OperationReporter
      .report("test", List("input"), OperationTypes.Patients, data.toDF, basePath)
    val resultData = spark.read.parquet(Path(basePath, "test", "data").toString)

    // Then
    assert(resultMetadata == expectedMetadata)
    assertDFs(resultData, expectedData)
  }

  it should "Write only the population for operation of OperationType.Source" in {
    import sqlCtx.implicits._

    // Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val basePath = Path("target/test/output")
    val expectedMetadata = OperationMetadata(
      "test", List("input"),
      OperationTypes.Sources,
      None,
      Some(Path(basePath, "test", "patients").toString)
    )
    val expectedPatients = Seq("Patient_A", "Patient_B").toDF("patientID")

    // When
    val resultMetadata: OperationMetadata = OperationReporter
      .report("test", List("input"), OperationTypes.Sources, data.toDF, basePath)
    val resultPatients = spark.read.parquet(Path(basePath, "test", "patients").toString)

    // Then
    assert(resultMetadata == expectedMetadata)
    assertDFs(resultPatients, expectedPatients)
  }

  it should "write meta data in a predefined place" in {
    import sqlCtx.implicits._
    // Given
    val basePath = Path("target/test/output")
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val optionMetadata: OperationMetadata = OperationReporter
      .report("test", List("input"), OperationTypes.Sources, data.toDF, basePath)
    val expectedMetadata = MainMetadata(
      this.getClass.getName,
      new java.util.Date(),
      new java.util.Date(),
      List(optionMetadata)
    ).toJsonString()

    // When
    OperationReporter.writeMetaData(expectedMetadata, "metadata_env1.json", "env1")
    OperationReporter.writeMetaData(expectedMetadata, "metadata_test.json", "test")
    val metadataFileInEnv1 = new File("metadata_env1.json")
    val metadataFileInTest = new File("metadata_test.json")
    val metadata = Source.fromFile("metadata_env1.json").getLines().mkString("\n")

    //Then
    assert(metadataFileInEnv1.exists() && metadataFileInEnv1.isFile)
    assert(expectedMetadata == metadata)
    assert(!metadataFileInTest.exists())

    metadataFileInEnv1.delete()

  }


}
