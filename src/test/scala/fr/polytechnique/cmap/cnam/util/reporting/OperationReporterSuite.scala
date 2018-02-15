package fr.polytechnique.cmap.cnam.util.reporting

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.Path
import org.apache.spark.sql.SQLContext

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
    val result: OperationMetadata = OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path)

    // Then
    assert(result == expected)
  }

  it should "Write the data correctly" in {
    import sqlCtx.implicits._

    // Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = Path("target/test/output")
    val expected = data

    // When
    OperationReporter.report("test", List("input"), OperationTypes.AnyEvents, data.toDF, path)
    val result = spark.read.parquet(Path(path, "test", "data").toString)

    // Then
    assertDFs(result, expected)
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
    val resultMetadata: OperationMetadata = OperationReporter.report("test", List("input"), OperationTypes.Patients, data.toDF, basePath)
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
    val resultMetadata: OperationMetadata = OperationReporter.report("test", List("input"), OperationTypes.Sources, data.toDF, basePath)
    val resultPatients = spark.read.parquet(Path(basePath, "test", "patients").toString)

    // Then
    assert(resultMetadata == expectedMetadata)
    assertDFs(resultPatients, expectedPatients)
  }
}
