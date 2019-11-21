package fr.polytechnique.cmap.cnam.etl.extractors.takeOverReasons

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

class HadTakeOveReasonSuite extends SharedContext {
  
  "extract" should "return a DataSet of HadMainTakeOveReasons" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val takeOverReasonCodes = Set("1")
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val expected = Seq[Event[MedicalTakeOverReason]](
      HadMainTakeOver("patient01", "10000123_30000123_2019", "1", makeTS(2019, 11, 21))
    ).toDS

    val input = Sources(had = Some(had))
    // When
    val result = HadMainTakeOverExtractor.extract(input, takeOverReasonCodes)

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
    val result = HadMainTakeOverExtractor.extract(input, Set.empty)

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
    val result = HadAssociatedTakeOverExtractor.extract(input, Set.empty)

    // Then
    assertDSs(result, expected)
  }
}
