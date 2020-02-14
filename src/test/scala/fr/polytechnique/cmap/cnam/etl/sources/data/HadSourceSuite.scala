package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class HadSourceSuite extends SharedContext {
  "sanitize" should "return lines that are not corrupted" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      HadSource.NIR_RET,
      HadSource.SEJ_RET,
      HadSource.FHO_RET,
      HadSource.PMS_RET,
      HadSource.DAT_RET,
      HadSource.ETA_NUM_EPMSI
    ).map(col => col.toString)

    val input = Seq(
      ("1", "1", "1", "1", "1", "1000100016"),
      ("1", "1", "1", "1", "1", "1000100016"),
      ("1", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "750019648")
    ).toDF(colNames: _*)

    val expected = Seq(
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016"),
      ("0", "0", "0", "0", "0", "1000100016")
    ).toDF(colNames: _*)

    // When
    val result = HadSource.sanitize(input)

    // Then
    assertDFs(result, expected)
  }
}
