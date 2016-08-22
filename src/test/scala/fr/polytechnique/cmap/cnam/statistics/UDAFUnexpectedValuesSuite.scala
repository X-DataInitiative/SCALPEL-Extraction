package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame

/**
  * Created by sathiya on 08/08/16.
  */
class UDAFUnexpectedValuesSuite extends Config {

  "UDAFUnexpectedValues" should "return correct output" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val sourceDF = getSourceDF
    val expectedValues: Set[String] = Set("CODE1234")
    val udaf = new UDAFUnexpectedValues(expectedValues)
    val columnName: String = "ORG_CLE_NEW"

    // When
    val resultDF = sourceDF.agg(udaf(sourceDF(columnName)))

    // Then
    assert(resultDF.first().get(0) == 0)
  }

  it should "return correct output when the columns contain null values" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given a DF with Null values
    val sourceDF = getSourceDF
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    val combinedDFWithNull = sourceDF.select("BEN_DTE_INS").unionAll(testDF)
    val expectedValues: Set[Int] = (1950 to 1960).toSet
    val udaf = new UDAFUnexpectedValues(expectedValues)
    val columnName: String = "BEN_DTE_INS"

    // When ("Applying on a Integer column")
    val resultDF: DataFrame = combinedDFWithNull.agg(udaf(combinedDFWithNull(columnName)))

    // Then
    assert(resultDF.first().get(0) == 5)
  }

  it should "return correct output when we expect double type on integer columns" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given a DF with Null values
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    val expectedValues: Set[Double] = Set(1970.0)
    val udafUnexpectedDouble = new UDAFUnexpectedValues(expectedValues)

    // When("Applying on a Integer column expecting a double value")
    val resultDF: DataFrame = testDF.agg(udafUnexpectedDouble($"BEN_DTE_INS"))

    // Then
    assert(resultDF.first().get(0) == 6)
  }

  it should "return correct output when the column type is Double" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given a DF with Null values
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS").select($"BEN_DTE_INS" cast ("double"))
    val expectedValues: Set[Double] = Set(1970.0)
    val udafUnexpectedDouble = new UDAFUnexpectedValues(expectedValues)

    // When("Applying on a Integer column expecting a double value")
    val resultDF: DataFrame = testDF.agg(udafUnexpectedDouble($"BEN_DTE_INS"))

    // Then
    assert(resultDF.first().get(0) == 3)
  }

  it should "return correct output when the column type is Long" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given a DF with Null values
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS").select($"BEN_DTE_INS" cast ("long"))
    val expectedValues: Set[Long] = Set(1970L)
    val udafUnexpectedLong = new UDAFUnexpectedValues(expectedValues)

    // When("Applying on a Integer column expecting a double value")
    val resultDF: DataFrame = testDF.agg(udafUnexpectedLong($"BEN_DTE_INS"))

    // Then
    assert(resultDF.first().get(0) == 3)
  }
}
