package fr.polytechnique.cmap.cnam.statistics

/**
  * Created by sathiya on 08/08/16.
  */
class UDAFUnexpectedValuesSuite extends Config {

  "UDAFUnexpectedValues" should "return correct output" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given
    val sourceDF = getSourceDF
    val udafUnexpectedString = new UDAFUnexpectedValues(Set("CODE1234"))

    // Then
    assert(sourceDF.agg(udafUnexpectedString(sourceDF("ORG_CLE_NEW"))).first().get(0) == 0)
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
      sourceDF.select("BEN_DTE_INS").unionAll(testDFWithNull)
    }

    // When ("Applying on a Integer column")
    val udafUnexpected = new UDAFUnexpectedValues((1950 to 1960).toSet)

    // Then
    assert(combinedDFWithNull.agg(udafUnexpected(combinedDFWithNull("BEN_DTE_INS")))
      .first().get(0) == 5)
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
    val udafUnexpectedDouble = new UDAFUnexpectedValues(Set(1970.0))

    // Then
    assert(combinedDFWithNull.agg(udafUnexpectedDouble($"BEN_DTE_INS"))
      .first().get(0) == 6)
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
    val udafUnexpectedDouble = new UDAFUnexpectedValues(Set(1970.0))

    // Then
    assert(combinedDFWithNull.select($"BEN_DTE_INS" cast ("double"))
      .agg(udafUnexpectedDouble($"BEN_DTE_INS"))
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
    val udafUnexpectedLong = new UDAFUnexpectedValues(Set(1970L))

    // Then
    assert(combinedDFWithNull.select($"BEN_DTE_INS" cast ("long"))
      .agg(udafUnexpectedLong($"BEN_DTE_INS"))
      .first().get(0) == 3)
  }

}
