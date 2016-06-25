package fr.polytechnique.cmap.cnam.statistics

/**
  * Created by sathiya on 29/07/16.
  */
class UDAFExpectAndUnexpecdValues extends CustomStatisticsSuite {

  import sqlContext.implicits._

  test("test"){

    // Given srcDF
    When("Applying on a String column")
    val udafUnexpected_string = new CustomAggUnexpectedValues(Set("CODE1234"))
    val udafExpected_string = new CustomAggExpectedValues(Set("CODE1234"))
    Then("The function should still work correctly")
    assert(srcDF.agg(udafUnexpected_string(srcDF("ORG_CLE_NEW"))).first().get(0) == 0)
    assert(srcDF.agg(udafExpected_string(srcDF("ORG_CLE_NEW"))).first().get(0) == 2)

    // Given a DF with Null values
    val testDF_withNull = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975)).toDF("BEN_DTE_INS")
    val combinedDF_withNull = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull)

    When("Applying on a Integer column")
    val udafUnexpected = new CustomAggUnexpectedValues((1950 to 1960).toSet)
    val udafExpected = new CustomAggExpectedValues((1950 to 1960).toSet)
    Then("The function should work correctly")
    assert(combinedDF_withNull.agg(udafUnexpected(combinedDF_withNull("BEN_DTE_INS"))).first().get(0) == 5)
    assert(combinedDF_withNull.agg(udafExpected(combinedDF_withNull("BEN_DTE_INS"))).first().get(0) == 1)

    When("Applying on a Integer column expecting a double value")
    val udafUnexpected_double = new CustomAggUnexpectedValues(Set(1970.0))
    val udafExpected_double = new CustomAggExpectedValues(Set(1970.0))
    Then("The function should still work correctly")
    assert(combinedDF_withNull.agg(udafUnexpected_double($"BEN_DTE_INS")).first().get(0) == 6)
    assert(combinedDF_withNull.agg(udafExpected_double($"BEN_DTE_INS")).first().get(0) == 0)

    When("Applying on a Double column")
    Then("The function should still work correctly")
    assert(combinedDF_withNull.select($"BEN_DTE_INS" cast("double")).agg(udafUnexpected_double($"BEN_DTE_INS")).first().get(0) == 3)
    assert(combinedDF_withNull.select($"BEN_DTE_INS" cast("double")).agg(udafExpected_double($"BEN_DTE_INS")).first().get(0) == 3)

    //Given some null Values
    When("Applying on a Integer column with null Values")
    val udafUnexpected_null = new CustomAggUnexpectedValues((1970 to 1975).toSet)
    val udafExpected_null = new CustomAggExpectedValues((1970 to 1975).toSet)
    Then("The function should still work correctly")
    assert(combinedDF_withNull.agg(udafUnexpected_null($"BEN_DTE_INS")).first().get(0) == 1)
    assert(combinedDF_withNull.agg(udafExpected_null($"BEN_DTE_INS")).first().get(0) == 5)
  }
}
