package fr.polytechnique.cmap.cnam.statistics

/**
  * Created by sathiya on 29/07/16.
  */
class UDAFAvgCountMaxMinNullSuite extends Config {

  "UDAFAvgSumMaxMinNullSuite" should "return correct output" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given
    val udafAllStatistics = new UDAFAvgSumMaxMinNull()
    val resultSpecified = {
      getSourceDF.select("BEN_RES_COM")
        .agg(udafAllStatistics($"BEN_RES_COM"))
        .persist
    }

    // When
    val result = resultSpecified.first().toSeq

    // then
    assert(result == Seq("(59.0,2,114,4,0)"))
  }
}
