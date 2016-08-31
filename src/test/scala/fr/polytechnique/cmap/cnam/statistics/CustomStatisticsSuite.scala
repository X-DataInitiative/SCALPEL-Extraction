package fr.polytechnique.cmap.cnam.statistics

import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
import org.apache.spark.sql.DataFrame

/**
  * Created by sathiya on 29/07/16.
  */

class CustomStatisticsSuite extends Config {

  import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._

  "customDescribe" should "compute statistics on all columns" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given

    val input = getSourceDF.select("BEN_CDI_NIR", "BEN_DTE_MAJ", "BEN_SEX_COD",
                                   "MAX_TRT_DTD", "ORG_CLE_NEW", "NUM_ENQ")
    val expected: DataFrame = {
      Seq(
        ("0", "0", 2L, 1L, "0", "0", "0.0", "0.0", "(0,2)", "(0,2)", "BEN_CDI_NIR"),
        ("01/01/2006", "25/01/2006", 2L, 2L, "NA", "NA", "NA", "NA", "(01/01/2006,1)(25/01/2006,1)", "(01/01/2006,1)(25/01/2006,1)", "BEN_DTE_MAJ"),
        ("1", "2", 2L, 2L, "3", "3", "1.5", "1.5", "(1,1)(2,1)", "(1,1)(2,1)", "BEN_SEX_COD"),
        ("07/03/2008", "07/03/2008", 1L, 1L, "NA", "NA", "NA", "NA", "(07/03/2008,1)", "(07/03/2008,1)", "MAX_TRT_DTD"),
        ("CODE1234", "CODE1234", 2L, 1L, "NA", "NA", "NA", "NA", "(CODE1234,2)", "(CODE1234,2)", "ORG_CLE_NEW"),
        ("Patient_01", "Patient_02", 2L, 2L, "NA", "NA", "NA", "NA", "(Patient_01,1)(Patient_02,1)", "(Patient_01,1)(Patient_02,1)", "NUM_ENQ")
      ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "AvgDistinct", "MaxOccur", "MinOccur", "ColName")
    }

    // When
    val result = input.customDescribe(forComparison = false)

    // Then
    assert(expected === result)

  }

  it should "compute statistics on specified columns" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDF = getSourceDF
    val cols = Array("BEN_TOP_CNS", "BEN_DCD_DTE", "NUM_ENQ")
    val expected = {
      Seq(
      ("1", "1", 2L, 1L, "2", "1", "1.0", "1.0", "(1,2)", "(1,2)", "BEN_TOP_CNS"),
      ("25/01/2008", "25/01/2008", 1L, 1L, "NA", "NA", "NA", "NA", "(25/01/2008,1)", "(25/01/2008,1)", "BEN_DCD_DTE"),
      ("Patient_01", "Patient_02", 2L, 2L, "NA", "NA", "NA", "NA", "(Patient_01,1)(Patient_02,1)", "(Patient_01,1)(Patient_02,1)", "NUM_ENQ")
      ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "AvgDistinct", "MaxOccur", "MinOccur", "ColName")
    }

    // When
    val resultColumns = givenDF.customDescribe(forComparison = false, cols: _*)

    // Then
    assert(resultColumns === expected)
  }

  it should "throw an exception" in {

    // Given
    val givenDF = getSourceDF
    val invalidCols = Array("NUM_ENQ", "INVALID_COLUMN")

    // When
    val thrown = intercept[java.lang.IllegalArgumentException] {
      givenDF.customDescribe(invalidCols: _*).count
    }

    // Then
    assert(thrown.getMessage.matches("Field \"[^\"]*\" does not exist."))
  }


  "countUnexpectedValues && countExpectedValues functions " should "return expected values" in {

    // Given
    val givenDF = getSourceDF
    val columnName: String = "ORG_CLE_NEW"
    val expectedValues: Set[String] = Set("CODE1234")

    // When applying it on a column of String type
    val resultUnexpectedValues = givenDF.countUnexpectedValues(columnName, expectedValues)
    val resultExpectedValues = givenDF.countExpectedValues(columnName, expectedValues)

    // Then
    assert(resultUnexpectedValues.first().get(0) == 0)
    assert(resultExpectedValues.first().get(0) == 2)
  }

  it should "return correct output when the columns contain null values" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given a DF with Null values
    val testDF = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDfWithNull = {
      getSourceDF.select("BEN_DTE_INS")
        .unionAll(testDF)
    }
    val columnName: String = "BEN_DTE_INS"
    val expectedValues: Set[Int] = (1950 to 1960).toSet

    // When applying it on a column of Integer type
    val resultUnexpectedValues = combinedDfWithNull.countUnexpectedValues(
       columnName, expectedValues)

    val resultExpectedValues = combinedDfWithNull.countExpectedValues(
      columnName, expectedValues)

    // Then
    assert(resultUnexpectedValues.first().get(0) == 5)
    assert(resultExpectedValues.first().get(0) == 1)
  }

  it should "return correct output when the column type is integer and expecting double values" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given a DF with Null values
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
      .toDF("BEN_DTE_INS")
    val columnName: String = "BEN_DTE_INS"
    val expectedValue: Set[Double] = Set(1970.0)

    // When applying it on a column of Integer type while expecting double value
    val resultUnexpectedValues = testDF.countUnexpectedValues(
      columnName, expectedValue)
    val resultExpectedValues = testDF.countExpectedValues(
      columnName, expectedValue)

    // Then
    assert(resultUnexpectedValues.first().get(0) == 6)
    assert(resultExpectedValues.first().get(0) == 0)

  }

  it should "return correct output when the column type is Double" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
      .toDF("BEN_DTE_INS").select($"BEN_DTE_INS" cast "double")
    val columnName: String = "BEN_DTE_INS"
    val expectedValues: Set[Double] = Set(1970.0)

    // When
    val resultUnexpectedValues = testDF.countUnexpectedValues(columnName, expectedValues)
    val resultExpectedValues = testDF.countExpectedValues(columnName, expectedValues)

    // Then
    assert(resultUnexpectedValues.first().get(0) == 3)
    assert(resultExpectedValues.first().get(0) == 3)
  }

  "countUnexpectedValues && countExpectedValues using UDAF's" should "return expected values" in {

    //Given
    val df = getSourceDF
    val expectedValues: Set[String] = Set("CODE1234")
    val columnName: String = "ORG_CLE_NEW"

    //When
    val resultUnexpectedValues = df.countUnexpectedValuesUsingUDAF(expectedValues, columnName)
    val resultExpectedValues = df.countExpectedValuesUsingUDAF(expectedValues, columnName)

    // Then
    assert(resultUnexpectedValues.first().get(0) == 0)
    assert(resultExpectedValues.first().get(0) == 2)
  }


  it should "return correct output when the columns contain null values" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given a DF with Null values
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
      .toDF("BEN_DTE_INS")
    val combinedDfWithNull = getSourceDF.select("BEN_DTE_INS")
      .unionAll(testDF)
    val expectedValues: Set[Int] = (1950 to 1960).toSet
    val columnName: String = "BEN_DTE_INS"

    // When
    val resultUnexpectedValues = combinedDfWithNull.countUnexpectedValuesUsingUDAF(
      expectedValues, columnName)

    val resultExpectedValues = combinedDfWithNull.countExpectedValuesUsingUDAF(
      expectedValues, columnName)

    // Then
    assert(resultUnexpectedValues.first().get(0) == 5)
    assert(resultExpectedValues.first().get(0) == 1)
  }

  it should "return correct output when the column type is integer and expecting double values" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
      .toDF("BEN_DTE_INS")

    val expectedValues: Set[Double] = Set(1970.0)
    val columnName: String = "BEN_DTE_INS"

    // When
    val resultUnexpectedValues = testDF.countUnexpectedValuesUsingUDAF(expectedValues, columnName)
    val resultExpectedValues = testDF.countExpectedValuesUsingUDAF(expectedValues, columnName)


    // Then
    assert(resultUnexpectedValues.first().get(0) == 6)
    assert(resultExpectedValues.first().get(0) == 0)

  }

  it should "return correct output when the column type is Double" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS").select($"BEN_DTE_INS" cast("double"))
    val expectedValues: Set[Double] = Set(1970.0)
    val columnName: String = "BEN_DTE_INS"

    // When
    val resultUnexpectedValues = testDF
        .countUnexpectedValuesUsingUDAF(expectedValues, columnName)

    val resultExpectedValues = testDF
        .countExpectedValuesUsingUDAF(expectedValues, columnName)

    // Then
    assert(resultUnexpectedValues.first().get(0) == 3)
    assert(resultExpectedValues.first().get(0) == 3)
  }
}