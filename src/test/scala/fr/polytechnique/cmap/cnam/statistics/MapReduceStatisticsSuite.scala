package fr.polytechnique.cmap.cnam.statistics

/**
  * Created by sathiya on 29/07/16.
  */
class MapReduceStatisticsSuite extends Config {

  import _sqlContext.implicits._

  val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_NAI_ANN")
  val combinedDF = srcDF.select("BEN_NAI_ANN").unionAll(testDF)

  "Find Max/Min Occurrences" should "return correct output" in {

    // When combinedDF

    // Then
    assert(StatisticsUsingMapReduce.findMostOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2)
      == Seq((1970, 3), (1975, 2)))
    assert(StatisticsUsingMapReduce.findLeastOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2)
      == Seq((1959, 1), (1960, 1)))
  }

  it should "return correct output in corner cases such as" in {

    // When the column type is Integer
    val combinedDF_integer =  combinedDF.select($"BEN_NAI_ANN" cast "integer")

    // Then
    assert(StatisticsUsingMapReduce.findMostOccurrenceValues(combinedDF_integer, "BEN_NAI_ANN", 2)
      == Seq((1970, 3), (1975, 2)))
    assert(StatisticsUsingMapReduce.findLeastOccurrenceValues(combinedDF_integer, "BEN_NAI_ANN", 2)
      == Seq((1959, 1), (1960, 1)))

    // When the column type is Double
    val combinedDF_double = combinedDF.select($"BEN_NAI_ANN" cast "double")

    // Then
    assert(StatisticsUsingMapReduce.findMostOccurrenceValues(combinedDF_double, "BEN_NAI_ANN", 2)
      == Seq((1970, 3), (1975, 2)))
    assert(StatisticsUsingMapReduce.findLeastOccurrenceValues(combinedDF_double, "BEN_NAI_ANN", 2)
      == Seq((1959, 1), (1960, 1)))

    // When the column type is Long
    val combinedDF_long = combinedDF.select($"BEN_NAI_ANN" cast "long")

    // Then
    assert(StatisticsUsingMapReduce.findMostOccurrenceValues(combinedDF_long, "BEN_NAI_ANN", 2)
      == Seq((1970, 3), (1975, 2)))
    assert(StatisticsUsingMapReduce.findLeastOccurrenceValues(combinedDF_long, "BEN_NAI_ANN", 2)
      == Seq((1959, 1), (1960, 1)))


    // When there is null Values in the column
    val testDF_withNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDF_withNull = {
      srcDF.select("BEN_DTE_INS")
        .unionAll(testDF_withNull)
    }

    // Then
    assert(StatisticsUsingMapReduce.findMostOccurrenceValues(combinedDF_withNull, "BEN_DTE_INS", 2)
      .toList == Seq(("1970", 3), ("1975", 2)))
    assert(StatisticsUsingMapReduce.findLeastOccurrenceValues(combinedDF_withNull, "BEN_DTE_INS", 2)
      .toList == Seq(("1960", 1), ("1975", 2)))
  }
}
