// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.reporting

import java.io.File
import scala.io.Source
import org.apache.spark.sql.{AnalysisException, SQLContext}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.general.GeneralSource
import fr.polytechnique.cmap.cnam.util.{Path, functions}

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
      Path(path, "test", "data").toString,
      Path(path, "test", "patients").toString
    )

    // When
    val result: OperationMetadata = OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path)

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
    val result = spark.read.parquet(meta.outputPath)
    val exception = intercept[Exception] {
      OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path)
    }
    meta = OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path, "append")
    val resultAppend = spark.read.parquet(meta.outputPath)
    meta = OperationReporter
      .report("test", List("input"), OperationTypes.AnyEvents, data.toDF, pathWithTimestamp, "withTimestamp")
    val resultWithTimestamp = spark.read.parquet(meta.outputPath)


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
      Path(basePath, "test", "data").toString
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
      "",
      Path(basePath, "test", "patients").toString
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

  it should "report in the orc or parquet" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    //Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val pathParquet = Path("target/test/output/parquet")
    val pathOrc = Path("target/test/output/orc")
    val patients = Seq(
      Patient("Patient_A", 1, functions.makeTS(1990, 2, 10), None),
      Patient("Patient_B", 1, functions.makeTS(1992, 5, 15), None),
      Patient("Patient_C", 1, functions.makeTS(1995, 8, 20), None)
    ).toDS()
    val pathDSParquet = Path("target/test/output/ds/parquet")
    val pathDSOrc = Path("target/test/output/ds/orc")
    //When
    var meta = OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, pathParquet, "overwrite")
    val result1 = GeneralSource.read(sqlCtx, meta.outputPath)
    meta = OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, pathOrc, "overwrite", "orc")
    val result2 = GeneralSource.read(sqlCtx, meta.outputPath, "orc")
    meta = OperationReporter.reportAsDataSet("test", List("input"), OperationTypes.AnyEvents, patients, pathDSParquet, "overwrite")
    val result3 = GeneralSource.read(sqlCtx, meta.outputPath).as[Patient]
    meta = OperationReporter.reportAsDataSet("test", List("input"), OperationTypes.AnyEvents, patients, pathDSOrc, "overwrite", "orc")
    val result4 = GeneralSource.read(sqlCtx, meta.outputPath, "orc").as[Patient]
    //Then
    assertDFs(data, result1)
    assertDFs(data, result2)
    assertDSs(patients, result3)
    assertDSs(patients, result4)

  }


}
