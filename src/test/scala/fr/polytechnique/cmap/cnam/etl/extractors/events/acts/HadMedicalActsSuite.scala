package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, HadCCAMAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class HadMedicalActsSuite extends SharedContext {

  "extract" should "return a DataSet of HadCCAMActs" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ccamCodes = SimpleExtractorCodes(List("HPQD001"))
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val expected = Seq[Event[MedicalAct]](
      HadCCAMAct("patient02", "10000201_30000150_2019", "HPQD001", makeTS(2019, 12, 24)),
      HadCCAMAct("patient01", "10000123_30000123_2019", "HPQD001", makeTS(2019, 11, 21))
    ).toDS

    val input = Sources(had = Some(had))
    // When
    val result = HadCcamActExtractor(ccamCodes).extract(input)

    // Then
    assertDSs(result, expected)
  }

  it should "return all available HadCCAMActs when Codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val expected = Seq[Event[MedicalAct]](
      HadCCAMAct("patient01", "10000123_30000124_2019", "HPQD132", makeTS(2019, 10, 10)),
      HadCCAMAct("patient02", "10000201_30000150_2019", "HPQD001", makeTS(2019, 12, 24)),
      HadCCAMAct("patient01", "10000123_30000123_2019", "HPQD001", makeTS(2019, 11, 21))
    ).toDS

    val input = Sources(had = Some(had))
    // When
    val result = HadCcamActExtractor(SimpleExtractorCodes.empty).extract(input)

    // Then
    assertDSs(result, expected)
  }
}
