package fr.polytechnique.cmap.cnam.etl.extractors.patients

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.DataFrame

class SsrPatientsSuite extends SharedContext {

  "transform" should "return correct Dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ssr: DataFrame = Seq(
      ("Patient_01"),
      ("Patient_02"),
      ("Patient_03"),
      ("Patient_03"),
      ("Patient_04")
    ).toDF("NUM_ENQ")

    val expected: DataFrame = Seq(
      ("Patient_01"),
      ("Patient_02"),
      ("Patient_03"),
      ("Patient_04")
    ).toDF("patientID")

    // When
    val result = SsrPatients.extract(ssr)

    // Then
    assertDFs(result.toDF, expected)
  }

  "extract" should "extract target SsrPatients" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")

    val result = SsrPatients.extract(ssr)

    val expected: DataFrame = Seq(
      ("Patient_01"),
      ("Patient_02")
    ).toDF("patientID")

    // Then
    assertDSs(result, expected)
  }
}