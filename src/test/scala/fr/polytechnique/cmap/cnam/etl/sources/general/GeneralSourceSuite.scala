package fr.polytechnique.cmap.cnam.etl.sources.general

import fr.polytechnique.cmap.cnam.SharedContext

class GeneralSourceSuite extends SharedContext {

  "read" should "get the data from Parquet or ORC" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(("A", 1, 0), ("B", 0, 71), ("C", 10, 3), ("D", 0, 4)).toDF("A", "B", "C")

    //When
    input.write.parquet("target/test/output/parquet_read_test/data")
    input.write.orc("target/test/output/orc_read_test/data")

    //Then
    val parquet = GeneralSource.read(sqlCtx, "target/test/output/parquet_read_test/data")
    val orc = GeneralSource.read(sqlCtx, "target/test/output/orc_read_test/data", "orc")

    assertDFs(input, parquet)
    assertDFs(input, orc)

  }

}
