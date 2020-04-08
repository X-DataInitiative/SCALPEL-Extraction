package fr.polytechnique.cmap.cnam.etl.extractors.events.takeoverreasons

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, HadAssociatedTakeOver, HadMainTakeOver, MedicalTakeOverReason}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

class HadTakeOveReasonSuite extends SharedContext {

  "extract" should "return a DataSet of HadMainTakeOveReasons" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val takeOverReasonCodes = SimpleExtractorCodes(List("1"))
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val expected = Seq[Event[MedicalTakeOverReason]](
      HadMainTakeOver("patient01", "10000123_30000123_2019", "1", makeTS(2019, 11, 21))
    ).toDS

    val input = Sources(had = Some(had))
    // When
    val result = HadMainTakeOverExtractor(takeOverReasonCodes).extract(input)

    // Then
    assertDSs(result, expected)
  }

  it should "return all available HadMainTakeOveReasons when Codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val expected = Seq[Event[MedicalTakeOverReason]](
      HadMainTakeOver("patient01", "10000123_30000124_2019", "2", makeTS(2019, 10, 10)),
      HadMainTakeOver("patient02", "10000201_30000150_2019", "10", makeTS(2019, 12, 24)),
      HadMainTakeOver("patient01", "10000123_30000123_2019", "1", makeTS(2019, 11, 21))
    ).toDS

    val input = Sources(had = Some(had))
    // When
    val result = HadMainTakeOverExtractor(SimpleExtractorCodes.empty).extract(input)

    // Then
    assertDSs(result, expected)
  }

  it should "return all available HadAssociatedTakeOveReasons when Codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val expected = Seq[Event[MedicalTakeOverReason]](
      HadAssociatedTakeOver("patient01", "10000123_30000124_2019", "2", makeTS(2019, 10, 10)),
      HadAssociatedTakeOver("patient02", "10000201_30000150_2019", "10", makeTS(2019, 12, 24)),
      HadAssociatedTakeOver("patient01", "10000123_30000123_2019", "1", makeTS(2019, 11, 21))
    ).toDS

    val input = Sources(had = Some(had))
    // When
    val result = HadAssociatedTakeOverExtractor(SimpleExtractorCodes.empty).extract(input)

    // Then
    assertDSs(result, expected)
  }
}
