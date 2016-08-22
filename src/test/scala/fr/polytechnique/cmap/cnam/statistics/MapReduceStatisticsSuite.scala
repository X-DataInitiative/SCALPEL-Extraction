package fr.polytechnique.cmap.cnam.statistics

import fr.polytechnique.cmap.cnam.Statistics

/**
  * Created by sathiya on 29/07/16.
  */
class MapReduceStatisticsSuite extends CustomStatisticsSuite{

  import sqlContext.implicits._

  test("Find Max/Min Occurrences") {

    When("I find max/min occurrence value in a column")
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_NAI_ANN")
    val combinedDF = srcDF.select("BEN_NAI_ANN").unionAll(testDF) //.select($"BEN_NAI_ANN")

    Then("I should get the right output")
    assert(Statistics.findMostOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2) == Seq((1960,1), (1969,1)))

    When("The column type is Integer")
    val combinedDF_integer = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN" cast "integer")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_integer, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF_integer, "BEN_NAI_ANN", 2) == Seq((1960,1), (1969,1)))

    When("The column type is Double")
    val combinedDF_double = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN" cast "double")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_double, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF_double, "BEN_NAI_ANN", 2) == Seq((1960,1), (1969,1)))

    When("The column type is Long")
    val combinedDF_long = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN" cast "long")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_long, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF_long, "BEN_NAI_ANN", 2) == Seq((1960,1), (1969,1)))

    When("There is null Values in the colum ")
    val testDF_withNull = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975)).toDF("BEN_DTE_INS")
    val combinedDF_withNull = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull) //.select($"BEN_DTE_INS")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_withNull, "BEN_DTE_INS", 2).toList == Seq(("1970", 3), ("1975", 2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF_withNull, "BEN_DTE_INS", 2).toList == Seq(("1960", 1), ("1975", 2)))

  }

}
