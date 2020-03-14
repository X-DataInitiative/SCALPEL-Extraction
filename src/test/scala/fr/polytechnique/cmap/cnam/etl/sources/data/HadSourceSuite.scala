package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class HadSourceSuite extends SharedContext {
  "sanitize" should "return lines that are not corrupted" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      HadSource.ENT_MOD,
      HadSource.SOR_MOD,
      HadSource.NIR_RET,
      HadSource.SEJ_RET,
      HadSource.FHO_RET,
      HadSource.PMS_RET,
      HadSource.DAT_RET,
      HadSource.ETA_NUM_EPMSI
    ).map(col => col.toString)

    val input = Seq(
      ("2", "3", "1", "0", "0", "0", "0", "100000000"),
      ("2", "3", "1", "1", "0", "0", "0", "100000001"),
      ("2", "3", "0", "0", "0", "0", "0", "100000001"),
      ("2", "3", "0", "0", "0", "0", "0", "910100015"),
      ("1", "1", "0", "0", "0", "0", "0", "100000001")

    ).toDF(colNames: _*)

    val expected = Seq(
      ("2", "3", "0", "0", "0", "0", "0", "100000001")
    ).toDF(colNames: _*)

    // When
    val result = HadSource.sanitize(input)

    // Then
    assertDFs(result, expected)
  }
}
