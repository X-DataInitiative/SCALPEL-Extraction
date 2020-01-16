package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class SsrDiagnosesSuite extends SharedContext {

  "extract" should "extract target SsrMainDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dpCodes = Set("C66")
    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")
    val sources = Sources(ssr = Some(ssr))

    val expected = Seq[Event[Diagnosis]](
      SsrMainDiagnosis("Patient_02", "10000123_30000546_200_2019", "C66", makeTS(2019, 8, 11)),
      SsrMainDiagnosis("Patient_02", "10000123_30000546_300_2019", "C66", makeTS(2019, 8, 11))
      //SsrMainDiagnosis("Patient_01", "10000123_30000801_100_2019", "C70", makeTS(2019, 10, 20))
    ).toDS


    // When
    val result = SsrMainDiagnosisExtractor.extract(sources, dpCodes)

    // Then
    assertDSs(result, expected)
  }

  it should "extract all available SsrMainDiagnosis when Codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")
    val sources = Sources(ssr = Some(ssr))

    val expected = Seq[Event[Diagnosis]](
      SsrMainDiagnosis("Patient_02", "10000123_30000546_200_2019", "C66", makeTS(2019, 8, 11)),
      SsrMainDiagnosis("Patient_02", "10000123_30000546_300_2019", "C66", makeTS(2019, 8, 11)),
      SsrMainDiagnosis("Patient_01", "10000123_30000801_100_2019", "C70", makeTS(2019, 10, 20))
    ).toDS


    // When
    val result = SsrMainDiagnosisExtractor.extract(sources, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  it should "extract target SsrLinkedDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val linkedCodes = Set("C6")
    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")
    val sources = Sources(ssr = Some(ssr))

    val expected = Seq[Event[Diagnosis]](
      SsrLinkedDiagnosis("Patient_02", "10000123_30000546_200_2019", "C68", makeTS(2019, 8, 11)),
      SsrLinkedDiagnosis("Patient_02", "10000123_30000546_300_2019", "C66", makeTS(2019, 8, 11))//,
      //SsrMainDiagnosis("Patient_01", "10000123_30000801_100_2019", "C55", makeTS(2019, 10, 20))
    ).toDS

    // When
    val result = SsrLinkedDiagnosisExtractor.extract(sources, linkedCodes)

    // Then
    assertDSs(result, expected)
  }

  it should "extract target SsrAssociatedDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val associatedDiagnosis = Set("C6")
    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")
    val sources = Sources(ssr = Some(ssr))

    val expected = Seq[Event[Diagnosis]](
      SsrAssociatedDiagnosis("Patient_02", "10000123_30000546_200_2019", "C66", makeTS(2019, 8, 11)),
      SsrAssociatedDiagnosis("Patient_02", "10000123_30000546_300_2019", "C68", makeTS(2019, 8, 11))
    ).toDS

    // When
    val result = SsrAssociatedDiagnosisExtractor.extract(sources, associatedDiagnosis)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return a DataSet of SsrTakingOverPurposes (cim10 codes)" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val cim10Codes = Set("Z100")
    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")
    val expected = Seq[Event[Diagnosis]](
      SsrTakingOverPurpose("Patient_02", "10000123_30000546_300_2019", "Z100", makeTS(2019, 8, 11))
    ).toDS

    val input = Sources(ssr = Some(ssr))
    // When
    val result = SsrTakingOverPurposeExtractor.extract(input, cim10Codes)

    // Then
    assertDSs(result, expected)
  }

  it should "return all available SsrTakingOverPurposes when codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")
    val expected = Seq[Event[Diagnosis]](
      SsrTakingOverPurpose("Patient_02", "10000123_30000546_200_2019", "Z400", makeTS(2019, 8, 11)),
      SsrTakingOverPurpose("Patient_02", "10000123_30000546_300_2019", "Z100", makeTS(2019, 8, 11)),
      SsrTakingOverPurpose("Patient_01", "10000123_30000801_100_2019", "Z200", makeTS(2019, 10, 20))
    ).toDS

    val input = Sources(ssr = Some(ssr))
    // When
    val result = SsrTakingOverPurposeExtractor.extract(input, Set.empty)

    // Then
    assertDSs(result, expected)
  }

}

