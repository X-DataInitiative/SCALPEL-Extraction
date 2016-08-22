package fr.polytechnique.cmap.cnam.statistics

/**
  * Created by sathiya on 29/07/16.
  */
class UDAFAvgCountMaxMinNullSuite extends CustomStatisticsSuite {

  import sqlContext.implicits._

  test("Test UDAFAvgSumMaxMinNull") {

    When("We compute UDAFAvgSumMaxMinNull")
    val udafAllStatistics = new UDAFAvgSumMaxMinNull()

    Then("we should get right output")
    val result_specified = srcDF.select("BEN_RES_COM").agg(udafAllStatistics($"BEN_RES_COM")).persist
    assert(result_specified.first().toSeq == Seq("(69.0,2,114,24,0)"))
  }
}
