package fr.polytechnique.cmap.cnam.statistics

/**
  * Created by sathiya on 29/07/16.
  */

class CustomStatisticsSuite extends Config {

  import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._

  "customDescribe" should "compute statistics on all columns" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given
    val givenDF = getSourceDF

    // When the column names are not specified
    val result = givenDF.customDescribe()

    // Then
    val expectedResult = {
      Seq(
        ("0", "0", 2L, "0.0", "0", "(0,2)", "(0,2)", "BEN_CDI_NIR"),
        (null, null, 0L, "NA", "NA", "", "", "BEN_DTE_INS"),
        ("01/01/2006", "25/01/2006", 2L, "NA", "NA", "(01/01/2006,1)", "(01/01/2006,1)", "BEN_DTE_MAJ"),
        ("1959", "1975", 2L, "1967.0", "3934", "(1959,1)", "(1959,1)", "BEN_NAI_ANN"),
        ("1", "10", 2L, "5.5", "11", "(1,1)", "(1,1)", "BEN_NAI_MOI"),
        ("4", "114", 2L, "59.0", "118", "(114,1)", "(114,1)", "BEN_RES_COM"),
        ("02A", "075", 2L, "NA", "NA", "(075,1)", "(075,1)", "BEN_RES_DPT"),
        ("1", "1", 2L, "1.0", "2", "(1,2)", "(1,2)", "BEN_RNG_GEM"),
        ("1", "2", 2L, "1.5", "3", "(1,1)", "(1,1)", "BEN_SEX_COD"),
        ("1", "1", 2L, "1.0", "2", "(1,2)", "(1,2)", "BEN_TOP_CNS"),
        ("07/03/2008", "07/03/2008", 1L, "NA", "NA", "(07/03/2008,1)", "(07/03/2008,1)", "MAX_TRT_DTD"),
        ("CODE1234", "CODE1234", 2L, "NA", "NA", "(CODE1234,2)", "(CODE1234,2)", "ORG_CLE_NEW"),
        ("CODE1234", "CODE1234", 2L, "NA", "NA", "(CODE1234,2)", "(CODE1234,2)", "ORG_AFF_BEN"),
        ("101", "200801", 2L, "100451.0", "200902", "(101,1)", "(101,1)", "BEN_DCD_AME"),
        ("25/01/2008", "25/01/2008", 1L, "NA", "NA", "(25/01/2008,1)", "(25/01/2008,1)", "BEN_DCD_DTE"),
        ("Patient_01", "Patient_02", 2L, "NA", "NA", "(Patient_01,1)", "(Patient_01,1)", "NUM_ENQ")
      ).toDF("Min", "Max", "Count", "Avg", "Sum", "MaxOccur", "MinOccur", "ColName")
    }

    assert(result.collect().toSet == expectedResult.collect().toSet)

  }

  it should "compute statistics on specified columns" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given
    val givenDF = getSourceDF

    // When
    val cols = Array("BEN_DCD_DTE", "NUM_ENQ")
    val resultColumns = givenDF.customDescribe(cols: _*)

    // Then
    val expectedResultColumns = {
      Seq(
        ("25/01/2008", "25/01/2008", 1L, "NA", "NA", "(25/01/2008,1)", "(25/01/2008,1)", "BEN_DCD_DTE"),
        ("Patient_01", "Patient_02", 2L, "NA", "NA", "(Patient_01,1)", "(Patient_01,1)", "NUM_ENQ")
      ).toDF("Min", "Max", "Count", "Avg", "Sum", "MaxOccur", "MinOccur", "ColName")
    }

    assert(resultColumns.collect().toSet == expectedResultColumns.collect().toSet)
  }

  it should "throw an exception" in {

    // Given
    val givenDF = getSourceDF

    // When
    val invalidCols = Array("NUM_ENQ", "INVALID_COLUMN")

    // Then
    val thrown = intercept[java.lang.IllegalArgumentException] {
      givenDF.customDescribe(invalidCols: _*).count
    }

    assert(thrown.getMessage.matches("Field \"[^\"]*\" does not exist."))
  }


  "countUnexpectedValues && countExpectedValues functions " should "return expected values" in {

    // Given
    val givenDF = getSourceDF

    // When applying it on a column of String type
    val resultUnexpectedValues = givenDF.countUnexpectedValues("ORG_CLE_NEW", Set("CODE1234"))
    val resultExpectedValues = givenDF.countExpectedValues("ORG_CLE_NEW", Set("CODE1234"))

    // Then
    assert(resultUnexpectedValues.first().get(0) == 0)
    assert(resultExpectedValues.first().get(0) == 2)
  }

  it should "return correct output when the columns contain null values" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val testDfWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDfWithNull = {
      getSourceDF.select("BEN_DTE_INS")
        .unionAll(testDfWithNull)
    }

    // When applying it on a column of Integer type
    val resultUnexpectedValues = combinedDfWithNull.countUnexpectedValues(
      "BEN_DTE_INS", (1950 to 1960).toSet)

    val resultExpectedValues = combinedDfWithNull.countExpectedValues(
      "BEN_DTE_INS", (1950 to 1960).toSet)

    // Then
    assert(resultUnexpectedValues.first().get(0) == 5)
    assert(resultExpectedValues.first().get(0) == 1)
  }

  it should "return correct output when the column type is integer and expecting double values" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val testDfWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDfWithNull = {
      getSourceDF.select("BEN_DTE_INS")
        .unionAll(testDfWithNull)
    }

    // When applying it on a column of Integer type while expecting double value
    val resultUnexpectedValues = combinedDfWithNull.countUnexpectedValues(
      "BEN_DTE_INS", Set(1970.0))
    val resultExpectedValues = combinedDfWithNull.countExpectedValues(
      "BEN_DTE_INS", Set(1970.0))


    // Then
    assert(resultUnexpectedValues.first().get(0) == 6)
    assert(resultExpectedValues.first().get(0) == 0)

  }

  it should "return correct output when the column type is Double" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val testDfWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDfWithNull = {
      getSourceDF.select("BEN_DTE_INS")
        .unionAll(testDfWithNull)
    }

    // When applying it on a column of Double type
    val resultUnexpectedValues = {
    combinedDfWithNull.select($"BEN_DTE_INS" cast("double"))
      .countUnexpectedValues("BEN_DTE_INS", Set(1970.0))
    }

    val resultExpectedValues = {
      combinedDfWithNull.select($"BEN_DTE_INS" cast("double"))
        .countExpectedValues("BEN_DTE_INS", Set(1970.0))
    }

    // Then
    assert(resultUnexpectedValues.first().get(0) == 3)
    assert(resultExpectedValues.first().get(0) == 3)
  }

  "countUnexpectedValues && countExpectedValues using UDAF's" should "return expected values" in {

    //Given
    val givenDF = getSourceDF

    //When
    val resultUnexpectedValues = getSourceDF.countUnexpectedValuesUsingUDAF(
      Set("CODE1234"), "ORG_CLE_NEW")
    val resultExpectedValues = getSourceDF.countExpectedValuesUsingUDAF(
      Set("CODE1234"), "ORG_CLE_NEW")

    // Then
    assert(resultUnexpectedValues.first().get(0) == 0)
    assert(resultExpectedValues.first().get(0) == 2)
  }


  it should "return correct output when the columns contain null values" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val testDfWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDfWithNull = {
      getSourceDF.select("BEN_DTE_INS")
        .unionAll(testDfWithNull)
    }

    // When applying it on a column of Integer type
    val resultUnexpectedValues = combinedDfWithNull.countUnexpectedValuesUsingUDAF(
      (1950 to 1960).toSet, "BEN_DTE_INS")

    val resultExpectedValues = combinedDfWithNull.countExpectedValuesUsingUDAF(
      (1950 to 1960).toSet, "BEN_DTE_INS")

    // Then
    assert(resultUnexpectedValues.first().get(0) == 5)
    assert(resultExpectedValues.first().get(0) == 1)
  }

  it should "return correct output when the column type is integer and expecting double values" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val testDfWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDfWithNull = {
      getSourceDF.select("BEN_DTE_INS")
        .unionAll(testDfWithNull)
    }

    // When applying it on a column of Integer type while expecting double value
    val resultUnexpectedValues = combinedDfWithNull
      .countUnexpectedValuesUsingUDAF(
      Set(1970.0), "BEN_DTE_INS")

    val resultExpectedValues = combinedDfWithNull
      .countExpectedValuesUsingUDAF(
        Set(1970.0), "BEN_DTE_INS")


    // Then
    assert(resultUnexpectedValues.first().get(0) == 6)
    assert(resultExpectedValues.first().get(0) == 0)

  }

  it should "return correct output when the column type is Double" in {

    val _sqlContext = sqlContext
    import _sqlContext.implicits._

    // Given a DF with Null values
    val testDfWithNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDfWithNull = {
      getSourceDF.select("BEN_DTE_INS")
        .unionAll(testDfWithNull)
    }

    // When applying it on a column of Double type
    val resultUnexpectedValues = {
      combinedDfWithNull
        .select($"BEN_DTE_INS" cast("double"))
        .countUnexpectedValuesUsingUDAF(Set(1970.0), "BEN_DTE_INS")
    }

    val resultExpectedValues = {
      combinedDfWithNull
        .select($"BEN_DTE_INS" cast("double"))
        .countExpectedValuesUsingUDAF(Set(1970.0), "BEN_DTE_INS")
    }

    // Then
    assert(resultUnexpectedValues.first().get(0) == 3)
    assert(resultExpectedValues.first().get(0) == 3)
  }
}