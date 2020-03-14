// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class SsrSourceSuite extends SharedContext {
  "sanitize" should "return lines that are not corrupted" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      SsrSource.GRG_GME,
      SsrSource.NIR_RET,
      SsrSource.SEJ_RET,
      SsrSource.FHO_RET,
      SsrSource.PMS_RET,
      SsrSource.DAT_RET,
      SsrSource.ETA_NUM,

      SsrSource.ENT_MOD,
      SsrSource.SOR_MOD
    ).map(col => col.toString)

    val input = Seq(
      ("90XXXX", "1", "1", "1", "1", "1", "1", "2", "3"),
      ("27XXXX", "1", "1", "1", "1", "1", "2", "2", "3"),
      ("76XXXX", "0", "0", "0", "0", "0", "1", "2", "3"),
      ("76XXXX", "0", "0", "0", "0", "0", "1", "2", "3"),
      ("76XXXX", "0", "0", "0", "0", "0", "1", "1", "1"),
      ("28XXXX", "0", "0", "0", "0", "0", "1", "1", "1"),
      ("90XXXX", "0", "0", "0", "0", "0", "1", "2", "3"),
      ("76XXXX", "0", "0", "0", "0", "0", "910100023", "1", "1"),
      ("28XXXX", "0", "0", "0", "0", "0", "1", "2", "3"),
      ("28XXXX", "0", "0", "0", "0", "0", "130784234", "2", "3")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("76XXXX", "0", "0", "0", "0", "0", "1", "2", "3"),
      ("76XXXX", "0", "0", "0", "0", "0", "1", "2", "3"),
      ("28XXXX", "0", "0", "0", "0", "0", "1", "1", "1"),
      ("28XXXX", "0", "0", "0", "0", "0", "1", "2", "3")
    ).toDF(colNames: _*)

    // When
    val result = SsrSource.sanitize(input)

    // Then
    assertDFs(result, expected)
  }
}
