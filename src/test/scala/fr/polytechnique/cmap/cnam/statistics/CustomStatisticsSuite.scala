package fr.polytechnique.cmap.cnam.statistics

import fr.polytechnique.cmap.cnam.SharedContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSuite, GivenWhenThen}

/**
  * Created by sathiya on 29/07/16.
  */
class CustomStatisticsSuite extends SharedContext {

  val _sqlContext = sqlContext
  import _sqlContext.implicits._
  var srcDF : DataFrame = _

  override def beforeAll() {
    val srcFilePath: String = "src/test/resources/IR_BEN_R.csv"
  // The older versions of the csv package don't convert empty values to null.
  // So the functions should give consistent output whether or not
  // the csv package converts empty values to null.
     srcDF =  sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .option("treatEmptyValuesAsNulls", "true")
    .option("nullValue", "")
    .load(srcFilePath)
  }

  import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._

  "customDescribe" should "compute statistics on all columns" in {

    // When the column names are not specified
    val result = srcDF.customDescribe()

    // Then
    val expectedResult = {
      Seq(
        ("0", "0", 2L, "0.0", "0", "(0,2)", "(0,2)", "BEN_CDI_NIR"),
        (null, null, 0L, "NA", "NA", "", "", "BEN_DTE_INS"),
        ("01/01/2006", "25/01/2006", 2L, "NA", "NA", "(01/01/2006,1)", "(01/01/2006,1)", "BEN_DTE_MAJ"),
        ("1959", "1975", 2L, "1967.0", "3934", "(1959,1)", "(1959,1)", "BEN_NAI_ANN"),
        ("1", "10", 2L, "5.5", "11", "(1,1)", "(1,1)", "BEN_NAI_MOI"),
        ("24", "114", 2L, "69.0", "138", "(114,1)", "(114,1)", "BEN_RES_COM"),
        ("75", "92", 2L, "83.5", "167", "(75,1)", "(75,1)", "BEN_RES_DPT"),
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

    // When
    val cols = Array("BEN_DCD_DTE", "NUM_ENQ")
    val resultColumns = srcDF.customDescribe(cols: _*)

    // Then
    val expectedResultColumns = {
      Seq(
        ("25/01/2008", "25/01/2008", 1L, "NA", "NA", "(25/01/2008,1)", "(25/01/2008,1)", "BEN_DCD_DTE"),
        ("Patient_01", "Patient_02", 2L, "NA", "NA", "(Patient_01,1)", "(Patient_01,1)", "NUM_ENQ")
      ).toDF("Min", "Max", "Count", "Avg", "Sum", "MaxOccur", "MinOccur", "ColName")
    }

    assert(resultColumns.collect().toSet == expectedResultColumns.collect().toSet)
  }

  it should "throw an expection" in {

    // When
    val invalidCols = Array("NUM_ENQ", "INVALID_COLUMN")
    val thrown = intercept[java.lang.IllegalArgumentException] {
      srcDF.customDescribe(invalidCols: _*).count
    }

    // Then
    assert(thrown.getMessage.matches("Field \"[^\"]*\" does not exist."))
  }



  "countUnexpectedValues && countExpectedValues functions " should "return expected values" in {


    // When applying it on a column of String type
    assert(srcDF.countUnexpectedValues("ORG_CLE_NEW", Set("CODE1234")).first().get(0) == 0)
    assert(srcDF.countExpectedValues("ORG_CLE_NEW", Set("CODE1234")).first().get(0) == 2)

  }

  it should "return correct output in corner cases such as" in {

    // Given a DF with Null values
    val testDF_withNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDF_withNull = {
      srcDF.select("BEN_DTE_INS")
        .unionAll(testDF_withNull)
    }

    // When applying it on a column of Integer type
    assert(combinedDF_withNull.countUnexpectedValues("BEN_DTE_INS", (1950 to 1960).toSet)
      .first().get(0) == 5)
    assert(combinedDF_withNull.countExpectedValues("BEN_DTE_INS", (1950 to 1960).toSet)
      .first().get(0) == 1)


    // When applying it on a column of Integer type while expecting double value
    assert(combinedDF_withNull.countUnexpectedValues("BEN_DTE_INS", Set(1970.0))
      .first().get(0) == 6)
    assert(combinedDF_withNull.countExpectedValues("BEN_DTE_INS", Set(1970.0))
      .first().get(0) == 0)

    // When applying it on a column of Double type
    assert(combinedDF_withNull.select($"BEN_DTE_INS" cast("double"))
      .countUnexpectedValues("BEN_DTE_INS", Set(1970.0))
      .first().get(0) == 3)
    assert(combinedDF_withNull.select($"BEN_DTE_INS" cast("double"))
      .countExpectedValues("BEN_DTE_INS", Set(1970.0))
      .first().get(0) == 3)
  }

  "countUnexpectedValues && countExpectedValues using UDAF's" should "return expected values" in {

    assert(srcDF.countUnexpectedValuesUsingUDAF(Set("CODE1234"), "ORG_CLE_NEW")
      .first().get(0) == 0)
    assert(srcDF.countExpectedValuesUsingUDAF(Set("CODE1234"), "ORG_CLE_NEW")
      .first().get(0) == 2)
  }

  it should "also return coirrect output in corner cases" in {

    // Given a DF with Null values
    val testDF_withNull = {
      sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975))
        .toDF("BEN_DTE_INS")
    }
    val combinedDF_withNull = {
      srcDF.select("BEN_DTE_INS")
           .unionAll(testDF_withNull)
    }

    // When applying it on a column of Integer type
    assert(combinedDF_withNull.countUnexpectedValuesUsingUDAF((1950 to 1960).toSet, "BEN_DTE_INS")
      .first().get(0) == 5)
    assert(combinedDF_withNull.countExpectedValuesUsingUDAF((1950 to 1960).toSet, "BEN_DTE_INS")
      .first().get(0) == 1)

    // When applying it on a column of Integer type while expecting double value
    assert(combinedDF_withNull.countUnexpectedValuesUsingUDAF(Set(1970.0), "BEN_DTE_INS")
      .first().get(0) == 6)
    assert(combinedDF_withNull.countExpectedValuesUsingUDAF(Set(1970.0), "BEN_DTE_INS")
      .first().get(0) == 0)

    // When applying it on a column of Double type
    assert(combinedDF_withNull.select($"BEN_DTE_INS" cast("double"))
      .countUnexpectedValuesUsingUDAF(Set(1970.0), "BEN_DTE_INS")
      .first().get(0) == 3)
    assert(combinedDF_withNull.select($"BEN_DTE_INS" cast("double"))
      .countExpectedValuesUsingUDAF(Set(1970.0), "BEN_DTE_INS")
      .first().get(0) == 3)
  }

}
