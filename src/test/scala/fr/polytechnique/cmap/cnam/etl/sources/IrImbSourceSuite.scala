package fr.polytechnique.cmap.cnam.etl.sources

import fr.polytechnique.cmap.cnam.SharedContext

class IrImbSourceSuite extends SharedContext {

  "sanitize" should "remove lines where IMB_ALD_DTD is empty" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val colName = IrImbSource.IMB_ALD_DTD.toString
    val input = Seq("25/01/2006", "", "13/03/2006", "", "25/04/2006").toDF(colName)
    val expected = Seq("25/01/2006", "13/03/2006", "25/04/2006").toDF(colName)

    // When
    val result = IrImbSource.sanitize(input)

    // Then
    assertDFs(result, expected)
  }
}
