package fr.polytechnique.cmap.cnam.statistics


/**
  * Created by sathiya on 29/07/16.
  */
class StatisticsUsingMapReduceSuite extends Config {

  "Find Max/Min Occurrences" should "return correct output" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_NAI_ANN")
    val combinedDF = getSourceDF.select("BEN_NAI_ANN").unionAll(testDF)

    // When
    val resultMostOccurence = StatisticsUsingMapReduce.findMostOccurrenceValues(
      combinedDF, "BEN_NAI_ANN")

    val resultLeastOccurence = StatisticsUsingMapReduce.findLeastOccurrenceValues(
      combinedDF, "BEN_NAI_ANN")

    // Then
    assert(resultMostOccurence == Seq((1970, 3)))
    assert(resultLeastOccurence == Seq((1959, 1), (1960, 1)))
  }

  it should "return correct output when the column type is String" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val testDF = sc.parallelize(List("1970", "1975", "1970", "1960", "1970")).toDF("BEN_NAI_ANN")
    val combinedDFString = getSourceDF.select($"BEN_NAI_ANN" cast "string").unionAll(testDF)

    // When
    val resultMostOccurrence = StatisticsUsingMapReduce.findMostOccurrenceValues(
      combinedDFString, "BEN_NAI_ANN")
    val resultLeastOccurrence = StatisticsUsingMapReduce.findLeastOccurrenceValues(
      combinedDFString, "BEN_NAI_ANN")

    // Then
    assert(resultMostOccurrence == Seq(("1970", 3)))
    assert(resultLeastOccurrence == Seq(("1959", 1), ("1960", 1)))

  }

  it should "return correct output when the column type is Double" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_NAI_ANN")
      .select($"BEN_NAI_ANN" cast "double")
    val combinedDFDouble = getSourceDF.select($"BEN_NAI_ANN" cast "double").unionAll(testDF)

    // When
    val resultMostOccurrence = StatisticsUsingMapReduce.findMostOccurrenceValues(
      combinedDFDouble, "BEN_NAI_ANN")

    val resultLeastOccurrence = StatisticsUsingMapReduce.findLeastOccurrenceValues(
      combinedDFDouble, "BEN_NAI_ANN")

    // Then
    assert(resultMostOccurrence == Seq((1970, 3)))
    assert(resultLeastOccurrence == Seq((1959, 1), (1960, 1)))

  }

  it should "return correct output when the column type is Long" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given combinedDF
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_NAI_ANN")
      .select($"BEN_NAI_ANN" cast "long")
    val combinedDFLong = getSourceDF.select($"BEN_NAI_ANN" cast "long").unionAll(testDF)

    // When
    val resultMostOccurrence = StatisticsUsingMapReduce.findMostOccurrenceValues(
      combinedDFLong, "BEN_NAI_ANN")
    val resultLeastOccurrence = StatisticsUsingMapReduce.findLeastOccurrenceValues(
      combinedDFLong, "BEN_NAI_ANN")

    // Then
    assert(resultMostOccurrence == Seq((1970, 3)))
    assert(resultLeastOccurrence == Seq((1959, 1), (1960, 1)))

  }

  it should "return correct output when the columns contain null values" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given there is null Values in the column
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
      .toDF("BEN_DTE_INS")
    val combinedDfWithNull = getSourceDF.select("BEN_DTE_INS")
      .unionAll(testDF)

    // When
    val resultMostOccurrence = StatisticsUsingMapReduce.findMostOccurrenceValues(
      combinedDfWithNull, "BEN_DTE_INS")
    val resultLeastOccurrence = StatisticsUsingMapReduce.findLeastOccurrenceValues(
      combinedDfWithNull, "BEN_DTE_INS")

    // Then
    assert(resultMostOccurrence.toList == Seq(("1970", 3)))
    assert(resultLeastOccurrence.toList == Seq(("1960", 1)))
  }
}
