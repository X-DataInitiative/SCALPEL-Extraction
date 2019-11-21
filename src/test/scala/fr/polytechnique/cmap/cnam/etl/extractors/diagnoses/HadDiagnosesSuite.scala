package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class HadDiagnosesSuite extends SharedContext {

  "extract" should "extract target HadMainDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dpCodes = Set("G970")
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val sources = Sources(had = Some(had))

    val expected = Seq[Event[Diagnosis]](
      HadMainDiagnosis("patient01", "10000123_30000124_2019", "G970", makeTS(2019, 10, 10)),
      HadMainDiagnosis("patient02", "10000201_30000150_2019", "G970", makeTS(2019, 12, 24))
    ).toDS

    // When
    val result = HadMainDiagnosisExtractor.extract(sources, dpCodes)

    // Then
    assertDSs(result, expected)
  }

  it should "extract all available HadMainDiagnosis when Codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val sources = Sources(had = Some(had))

    val expected = Seq[Event[Diagnosis]](
      HadMainDiagnosis("patient01", "10000123_30000124_2019", "G970", makeTS(2019, 10, 10)),
      HadMainDiagnosis("patient02", "10000201_30000150_2019", "G970", makeTS(2019, 12, 24)),
      HadMainDiagnosis("patient01", "10000123_30000123_2019", "F1954", makeTS(2019, 11, 21))
    ).toDS

    // When
    val result = HadMainDiagnosisExtractor.extract(sources, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  it should "extract target HadAssociatedDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val associatedDiagnosis = Set("G9")
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val sources = Sources(had = Some(had))

    val expected = Seq[Event[Diagnosis]](
      HadAssociatedDiagnosis("patient01", "10000123_30000124_2019", "G97", makeTS(2019, 10, 10)),
      HadAssociatedDiagnosis("patient01", "10000123_30000123_2019", "G97", makeTS(2019, 11, 21)),
      HadAssociatedDiagnosis("patient02", "10000201_30000150_2019", "G969", makeTS(2019, 12, 24))
    ).toDS

    // When
    val result = HadAssociatedDiagnosisExtractor.extract(sources, associatedDiagnosis)

    // Then
    assertDSs(result, expected)
  }
}

