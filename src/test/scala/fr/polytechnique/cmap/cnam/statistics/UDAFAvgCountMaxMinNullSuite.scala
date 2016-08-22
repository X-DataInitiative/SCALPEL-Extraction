package fr.polytechnique.cmap.cnam.statistics

/**
  * Created by sathiya on 29/07/16.
  */
class UDAFAvgCountMaxMinNullSuite extends Config {

  "UDAFAvgSumMaxMinNullSuite" should "return correct output" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val udafAllStatistics = new UDAFAvgSumMaxMinNull()
    val columnName: String = "BEN_RES_COM"

    // When
    val resultDF = getSourceDF.select(columnName)
      .agg(udafAllStatistics($"BEN_RES_COM"))
    val result = resultDF.first().toSeq

    // then
    assert(result == Seq("(59.0,2,114,4,0)"))
  }
}
