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
    val dpCodes = Set("C67")
    val ssr = spark.read.parquet("src/test/resources/test-joined/SSR.parquet")
    val sources = Sources(ssr = Some(ssr))

    val expected = Seq[Event[Diagnosis]](
      SsrMainDiagnosis("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      SsrMainDiagnosis("Patient_02", "10000123_20000345_2007", "C671", makeTS(2007, 1, 29)),
      SsrMainDiagnosis("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 29)),
      SsrMainDiagnosis("Patient_02", "10000123_10000543_2006", "C671", makeTS(2005, 12, 24)),
      SsrMainDiagnosis("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 8)),
      SsrMainDiagnosis("Patient_02", "10000123_30000852_2008", "C671", makeTS(2008, 3, 15))
    ).toDS


    // When
    val result = SsrMainDiagnosisExtractor.extract(sources, dpCodes)

    // Then
    assertDSs(result, expected)
  }

  it should "extract target SsrLinkedDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val linkedCodes = Set("E05", "E08")
    val ssr = spark.read.parquet("src/test/resources/test-joined/SSR.parquet")
    val sources = Sources(ssr = Some(ssr))

    val expected = Seq[Event[Diagnosis]](
      SsrLinkedDiagnosis("Patient_02", "10000123_20000123_2007", "E05", makeTS(2007, 1, 29)),
      SsrLinkedDiagnosis("Patient_02", "10000123_10000543_2006", "E08", makeTS(2005, 12, 24))
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
    val associatedDiagnosis = Set("C66")
    val ssr = spark.read.parquet("src/test/resources/test-joined/SSR.parquet")
    val sources = Sources(ssr = Some(ssr))

    val expected = Seq[Event[Diagnosis]](
      SsrAssociatedDiagnosis("Patient_02", "10000123_20000123_2007", "C66.5", makeTS(2007, 1, 29)),
      SsrAssociatedDiagnosis("Patient_02", "10000123_10000543_2006", "C66.9", makeTS(2005, 12, 24))
    ).toDS

    // When
    val result = SsrAssociatedDiagnosisExtractor.extract(sources, associatedDiagnosis)

    // Then
    assertDSs(result, expected)
  }

}

