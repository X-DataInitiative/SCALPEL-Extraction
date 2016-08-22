package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame

/**
  * Created by sathiya on 29/07/16.
  */
class UDAFExpectedValuesSuite extends Config {

  "UDAFExpectedValues" should "return correct output" in {

    // Given
    val sourceDF = getSourceDF
    val expectedValues: Set[String] = Set("CODE1234")
    val udaf = new UDAFExpectedValues(expectedValues)
    val columnName: String = "ORG_CLE_NEW"

    // When
    val resultDF = sourceDF.agg(udaf(sourceDF(columnName)))

    // Then
    assert(resultDF.first().get(0) == 2)
  }

  it should "return correct output when the columns contain null values" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given a DF with Null values
    val sourceDF = getSourceDF
    val testDFWithNull = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
      .toDF("BEN_DTE_INS")
    val combinedDFWithNull = sourceDF.select("BEN_DTE_INS")
      .unionAll(testDFWithNull)

    val columnName: String = "BEN_DTE_INS"
    val expectedValues: Set[Int] = (1950 to 1960).toSet
    val udafExpected = new UDAFExpectedValues(expectedValues)

    // When ("Applying on a Integer column")
    val resultDF = combinedDFWithNull.agg(udafExpected(combinedDFWithNull(columnName)))

    // Then
    assert(resultDF.first().get(0) == 1)
  }

  it should "return correct output when we expect double type on integer columns" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
      .toDF("BEN_DTE_INS")
    val expectedValues: Set[Double] = Set(1970.0)
    val udaf = new UDAFExpectedValues(expectedValues)

    // When("Applying on a Integer column expecting a double value")
    val resultDF = testDF.agg(udaf($"BEN_DTE_INS"))

    // Then
    assert(resultDF.first().get(0) == 0)
  }

  it should "return correct output when the column type is Double" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
      .toDF("BEN_DTE_INS").select($"BEN_DTE_INS" cast ("double"))
    val expectedValues: Set[Double] = Set(1970.0)
    val udaf = new UDAFExpectedValues(expectedValues)

    // When
    val resultDF = testDF.agg(udaf($"BEN_DTE_INS"))

    // Then
    assert(resultDF.first().get(0) == 3)
  }

  it should "return correct output when the column type is Long" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS").select($"BEN_DTE_INS" cast ("long"))
    val expectedValues: Set[Long] = Set(1970L)
    val udaf = new UDAFExpectedValues(expectedValues)

    // When
    val resultDF = testDF.agg(udaf($"BEN_DTE_INS"))

    // Then
    assert(resultDF.first().get(0) == 3)
  }
}