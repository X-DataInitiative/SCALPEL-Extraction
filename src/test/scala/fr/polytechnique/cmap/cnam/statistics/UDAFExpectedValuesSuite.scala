package fr.polytechnique.cmap.cnam.statistics

/**
  * Created by sathiya on 29/07/16.
  */
class UDAFExpectedValuesSuite extends Config {

  "UDAFExpectedValues" should "return correct output" in {

    // Given
    val sourceDF = getSourceDF
    val udafExpectedString = new UDAFExpectedValues(Set("CODE1234"))

    // When
    val resultDF = sourceDF.agg(udafExpectedString(sourceDF("ORG_CLE_NEW")))

    // Then
    assert(resultDF.first().get(0) == 2)
  }

  it should "return correct output when the columns contain null values" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val sourceDF = getSourceDF
    val testDFWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDFWithNull = {
      sourceDF.select("BEN_DTE_INS")
              .unionAll(testDFWithNull)
    }

    // When ("Applying on a Integer column")
    val udafExpected = new UDAFExpectedValues((1950 to 1960).toSet)

    // Then
    assert(combinedDFWithNull.agg(udafExpected(combinedDFWithNull("BEN_DTE_INS")))
      .first().get(0) == 1)
  }

  it should "return correct output when we expect double type on integer columns" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val sourceDF = getSourceDF
    val testDFWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDFWithNull = {
      sourceDF.select("BEN_DTE_INS").unionAll(testDFWithNull)
    }

    // When("Applying on a Integer column expecting a double value")
    val udafExpectedDouble = new UDAFExpectedValues(Set(1970.0))

    // Then
    assert(combinedDFWithNull.agg(udafExpectedDouble($"BEN_DTE_INS"))
      .first().get(0) == 0)
  }

  it should "return correct output when the column type is Double" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val sourceDF = getSourceDF
    val testDFWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDFWithNull = {
      sourceDF.select("BEN_DTE_INS").unionAll(testDFWithNull)
    }

    // When("Applying on a Integer column expecting a double value")
    val udafExpectedDouble = new UDAFExpectedValues(Set(1970.0))

    // Then
    assert(combinedDFWithNull.select($"BEN_DTE_INS" cast ("double"))
      .agg(udafExpectedDouble($"BEN_DTE_INS"))
      .first().get(0) == 3)
  }

  it should "return correct output when the column type is Long" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val sourceDF = getSourceDF
    val testDFWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDFWithNull = {
      sourceDF.select("BEN_DTE_INS").unionAll(testDFWithNull)
    }

    // When("Applying on a Integer column expecting a double value")
    val udafExpectedLong = new UDAFExpectedValues(Set(1970L))

    // Then
    assert(combinedDFWithNull.select($"BEN_DTE_INS" cast ("long"))
      .agg(udafExpectedLong($"BEN_DTE_INS"))
      .first().get(0) == 3)
  }
}