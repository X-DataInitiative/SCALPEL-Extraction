package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class SsrSourceSuite extends SharedContext {
  "sanitize" should "return lines that are not corrupted" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      SsrSource.ETA_NUM,
      SsrSource.RHA_NUM,
      SsrSource.RHS_NUM,
      SsrSource.MOR_PRP,
      SsrSource.ETL_AFF,
      SsrSource.MOI_ANN_SOR_SEJ,
      SsrSource.RHS_ANT_SEJ_ENT,
      SsrSource.FP_PEC,
      SsrSource.NIR_RET,
      SsrSource.SEJ_RET,
      SsrSource.FHO_RET,
      SsrSource.PMS_RET,
      SsrSource.DAT_RET,
      SsrSource.ENT_DAT,
      SsrSource.SOR_DAT,
      SsrSource.Year

    ).map(col => col.toString)

    val input = Seq(
      ("10000123", "20000123", "123", "C66", "C24", "200910", "14", "Z15", "1", "1", "1", "1", "1", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "123", "C66", "C24", "200910", "14", "Z15", "1", "1", "1", "1", "1", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "123", "C66", "C24", "200910", "14", "Z15", "1", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "124", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "125", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "126", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "127", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "128", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "129", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "130", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("10000123", "20000123", "124", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "125", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "126", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "127", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "128", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "129", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008"),
      ("10000123", "20000123", "130", "C66", "C24", "200910", "14", "Z15", "0", "0", "0", "0", "0", "14062008", "25082008", "2008")
    ).toDF(colNames: _*)

    // When
    val result = SsrSource.sanitize(input)

    // Then
    assertDFs(result, expected)
  }

  "readAnnotateJoin"  should "return annotated joined SSR given SSR_C and SSR_SEJ" in {
    val sqlCtx = sqlContext

    val ssrSejPath = "src/test/resources/test-input/SSR_SEJ.parquet"
    val ssrCPath = "src/test/resources/test-input/SSR_C.parquet"
    val expected = sqlCtx.read.parquet("src/test/resources/test-joined/SSR.parquet")
    val result = SsrSource.readAnnotateJoin(
      sqlCtx,
      List(ssrSejPath, ssrCPath),
    "SSR_C")

    assertDFs(result, expected)
  }
}
