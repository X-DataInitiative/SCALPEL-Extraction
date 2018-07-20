package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class DataSourceManagerSuite extends SharedContext{

  object TestSourceSanitizer extends DataSourceManager

  "sanitizeDates" should "keep lines that are within the study period" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = Seq(
      (makeTS(2015, 2, 1), "1"),
      (makeTS(2015, 3, 1), "2"),
      (makeTS(2014, 2, 1), "3"),
      (makeTS(2016, 2, 1), "4")
    ).toDF("EXE_SOI_DTD", "ID")
    val expected = Seq(
      (makeTS(2015, 2, 1), "1"),
      (makeTS(2015, 3, 1), "2")
    ).toDF("EXE_SOI_DTD", "ID")

    // When
    val result = TestSourceSanitizer.sanitizeDates(input, makeTS(2015, 1, 1), makeTS(2015, 12, 31))

    // Then
    assertDFs(result, expected)
  }
}
