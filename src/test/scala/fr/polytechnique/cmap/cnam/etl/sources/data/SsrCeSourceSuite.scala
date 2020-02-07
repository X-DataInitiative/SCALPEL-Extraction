package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class SsrCeSourceSuite extends SharedContext {
  "sanitize" should "return lines that are not corrupted" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      SsrCeSource.NIR_RET,
      SsrCeSource.NAI_RET,
      SsrCeSource.SEX_RET,
      SsrCeSource.IAS_RET,
      SsrCeSource.ENT_DAT_RET,
      SsrCeSource.ETA_NUM
    ).map(col => col.toString)

    val input = Seq(
      ("1", "1", "1", "1", "1", "3333"),
      ("1", "0", "1", "0", "1", "3424"),
      ("0", "0", "0", "0", "0", "8271"),
      ("0", "0", "0", "0", "0", "9999"),
      ("0", "0", "0", "0", "0", "910100023"),
      ("0", "0", "0", "0", "0", "910100024"),
      ("0", "0", "0", "0", "0", "130784235")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("0", "0", "0", "0", "0", "8271"),
      ("0", "0", "0", "0", "0", "9999"),
      ("0", "0", "0", "0", "0", "910100024"),
      ("0", "0", "0", "0", "0", "130784235")
    ).toDF(colNames: _*)

    // When
    val result = SsrCeSource.sanitize(input)

    // Then
    assertDFs(result, expected)
  }
}
