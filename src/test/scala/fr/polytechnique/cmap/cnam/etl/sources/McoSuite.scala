package fr.polytechnique.cmap.cnam.etl.sources

import fr.polytechnique.cmap.cnam.SharedContext

class McoSuite extends SharedContext {

  "read" should "remove irrelevant lines" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val path = "src/test/resources/extractors/dummy.parquet"

    val expected = Seq(("paris", "42")).toDF("ETA_NUM", "SEQ_NUM")

    // When
    val result = Mco.read(sqlCtx, path)

    // Then
    assertDFs(result, expected)
  }
}