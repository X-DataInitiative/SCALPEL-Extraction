package fr.polytechnique.cmap.cnam.statistics

/**
  * Created by sathiya on 29/07/16.
  */
class UDAFAvgCountMaxMinNullSuite extends Config {

  import _sqlContext.implicits._

  "Test UDAFAvgSumMaxMinNull" should "return correct output" in {

    // Given
    val udafAllStatistics = new UDAFAvgSumMaxMinNull()
    val result_specified = {
      srcDF.select("BEN_RES_COM")
        .agg(udafAllStatistics($"BEN_RES_COM"))
        .persist
    }

    // then
    assert(result_specified.first().toSeq == Seq("(69.0,2,114,24,0)"))
  }
}
