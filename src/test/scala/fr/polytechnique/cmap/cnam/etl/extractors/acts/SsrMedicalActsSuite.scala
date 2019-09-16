package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

class SsrMedicalActsSuite extends SharedContext {

  "extract" should "return a DataSet of SsrCIM10Act" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val cim10Codes = Set("Z100")
    val ssr = spark.read.parquet("src/test/resources/test-joined/SSR.parquet")
    val expected = Seq[Event[MedicalAct]](
      SsrCIM10Act("Patient_02", "10000123_30000546_300_2019", "Z100", makeTS(2019, 8, 11))
    ).toDS

    val input = Sources(ssr = Some(ssr))
    // When
    val result = SsrCimMedicalActExtractor.extract(input, cim10Codes)

    // Then
    assertDSs(result, expected)
  }

  it should "return all available SsrCIM10Act when codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ssr = spark.read.parquet("src/test/resources/test-joined/SSR.parquet")
    val expected = Seq[Event[MedicalAct]](
      SsrCIM10Act("Patient_02", "10000123_30000546_200_2019", "Z400", makeTS(2019, 8, 11)),
      SsrCIM10Act("Patient_02", "10000123_30000546_300_2019", "Z100", makeTS(2019, 8, 11)),
      SsrCIM10Act("Patient_01", "10000123_30000801_100_2019", "Z200", makeTS(2019, 10, 20))
    ).toDS

    val input = Sources(ssr = Some(ssr))
    // When
    val result = SsrCimMedicalActExtractor.extract(input, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return a DataSet of SsrCCAMActs" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ccamCodes = Set("AHQP001")
    val ssr = spark.read.parquet("src/test/resources/test-joined/SSR.parquet")
    val expected = Seq[Event[MedicalAct]](
      SsrCCAMAct("Patient_02", "10000123_30000546_200_2019", "AHQP001", makeTS(2019, 8, 11)),
      SsrCCAMAct("Patient_02", "10000123_30000546_300_2019", "AHQP001", makeTS(2019, 8, 11))
    ).toDS

    val input = Sources(ssr = Some(ssr))
    // When
    val result = SsrCcamActExtractor.extract(input, ccamCodes)

    // Then
    assertDSs(result, expected)
  }

  it should "return all available SsrCCAMActs when Codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ssr = spark.read.parquet("src/test/resources/test-joined/SSR.parquet")
    val expected = Seq[Event[MedicalAct]](
      SsrCCAMAct("Patient_02", "10000123_30000546_200_2019", "AHQP001", makeTS(2019, 8, 11)),
      SsrCCAMAct("Patient_01", "10000123_30000801_100_2019", "AHQP005", makeTS(2019, 10, 20)),
      SsrCCAMAct("Patient_02", "10000123_30000546_300_2019", "AHQP001", makeTS(2019, 8, 11))

    ).toDS

    val input = Sources(ssr = Some(ssr))
    // When
    val result = SsrCcamActExtractor.extract(input, Set.empty)

    // Then
    assertDSs(result, expected)
  }

}
