package fr.polytechnique.cmap.cnam.util.reporting

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.{Dataset, SQLContext}

class OperationReporterSuite extends SharedContext {

  lazy val sqlCtx: SQLContext = super.sqlContext

  lazy val patients: Dataset[Patient] = {
    import sqlCtx.implicits._
    Seq(
      Patient("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2010, 1, 1))),
      Patient("Patient_B", 0, makeTS(1960, 1, 1), None),
      Patient("Patient_C", 1, makeTS(1975, 1, 1), None)
    ).toDS
  }

  "report" should "Return the correct metadata" in {
    import sqlCtx.implicits._

    // Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = Path("target/test/output")
    val expected = OperationMetadata(
      "test", List("input"), Path(path, "test", "data").toString, 3, Some(Path(path,"test", "patients").toString), Some(2)
    )

    // When
    val result: OperationMetadata = OperationReporter.report("test", List("input"), data.toDF, path, Some(patients))

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
    OperationReporter.report("test", List("input"), data.toDF, path, Some(patients))
    val result = spark.read.parquet(Path(path, "test", "data").toString)

    // Then
    assertDFs(result, expected)
  }

  it should "Write the patients correctly" in {
    import sqlCtx.implicits._

    // Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = Path("target/test/output")
    val expected = patients.filter(p => Set("Patient_A", "Patient_B").contains(p.patientID)).toDF

    // When
    OperationReporter.report("test", List("input"), data.toDF, path, Some(patients))
    val result = spark.read.parquet(Path(path, "test", "patients").toString)

    // Then
    assertDFs(result, expected)
  }
}
