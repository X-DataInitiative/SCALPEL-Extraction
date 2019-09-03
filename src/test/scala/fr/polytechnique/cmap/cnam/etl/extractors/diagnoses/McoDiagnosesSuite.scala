package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoDiagnosesSuite extends SharedContext {

  "extract" should "extract target MainDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dpCodes = Set("C67")
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val sources = Sources(mco = Some(mco))

    val expected = Seq[Event[Diagnosis]](
      MainDiagnosis("Patient_02", "10000123_20000123_2007", "C670", 2.0, makeTS(2007, 1, 29)),
      MainDiagnosis("Patient_02", "10000123_20000345_2007", "C671", 2.0, makeTS(2007, 1, 29)),
      MainDiagnosis("Patient_02", "10000123_10000987_2006", "C670", 2.0, makeTS(2005, 12, 29)),
      MainDiagnosis("Patient_02", "10000123_10000543_2006", "C671", 2.0, makeTS(2005, 12, 24)),
      MainDiagnosis("Patient_02", "10000123_30000546_2008", "C670", 2.0, makeTS(2008, 3, 8)),
      MainDiagnosis("Patient_02", "10000123_30000852_2008", "C671", 2.0, makeTS(2008, 3, 15)),
      MainDiagnosis("Patient_02", "10000123_30000852_2008", "C671", 4.0, makeTS(2008, 3, 15)),
      MainDiagnosis("Patient_02", "10000123_30000852_2008", "C671", 3.0, makeTS(2008, 3, 15))
    ).toDS


    // When
    val result = MainDiagnosisExtractor.extract(sources, dpCodes)

    // Then
    assertDSs(result, expected)
  }

  it should "extract target LinkedDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val linkedCodes = Set("E05", "E08")
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val sources = Sources(mco = Some(mco))

    val expected = Seq[Event[Diagnosis]](
      LinkedDiagnosis("Patient_02", "10000123_20000123_2007", "E05", makeTS(2007, 1, 29)),
      LinkedDiagnosis("Patient_02", "10000123_10000543_2006", "E08", makeTS(2005, 12, 24))
    ).toDS

    // When
    val result = LinkedDiagnosisExtractor.extract(sources, linkedCodes)

    // Then
    assertDSs(result, expected)
  }

  it should "extract target AssocitedDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val associatedDiagnosis = Set("C66")
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val sources = Sources(mco = Some(mco))

    val expected = Seq[Event[Diagnosis]](
      AssociatedDiagnosis("Patient_02", "10000123_20000123_2007", "C66.5", 2.0, makeTS(2007, 1, 29)),
      AssociatedDiagnosis("Patient_02", "10000123_10000543_2006", "C66.9", 2.0, makeTS(2005, 12, 24))
    ).toDS

    // When
    val result = AssociatedDiagnosisExtractor.extract(sources, associatedDiagnosis)

    // Then
    assertDSs(result, expected)
  }

}

