package fr.polytechnique.cmap.cnam.filtering

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._

/**
  * Created by burq on 05/08/16.
  */
class McoExtractorSuite extends SharedContext {

  "extract" should "remove irrelevant lines" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val path = "src/test/resources/extractors/dummy.parquet"
    val mcoExtractor = new McoExtractor(sqlCtx)

    val expected = Seq(("paris", "42")).toDF("ETA_NUM", "SEQ_NUM")

    // When
    val result = mcoExtractor.extract(path)

    // Then
    assert(result === expected)

  }
}
