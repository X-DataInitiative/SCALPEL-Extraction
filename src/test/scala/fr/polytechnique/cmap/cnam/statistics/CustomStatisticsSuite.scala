package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSuite, GivenWhenThen}

/**
  * Created by sathiya on 29/07/16.
  */
class CustomStatisticsSuite extends FunSuite with GivenWhenThen {

  val sc = CustomStatistics.getSparkContext()

  test("Test getSparkContext Function"){
    assert(sc.isInstanceOf[SparkContext])
  }

  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val srcFilePath: String = "src/test/resources/IR_BEN_R.csv"
  val srcDF = { sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .option("treatEmptyValuesAsNulls", "true") // In the older version of the csv package, it is not possible to consider empty values as null.
    .option("nullValue", "")                   // So the functions should give consistent output whether or not the csv package converts empty values to null.
    .load(srcFilePath)
  }

  test("Test for Describe Function") {

    import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._

    val expectedResult = {
      Seq(
        ("0", "0", 2L, "0.0", "0", "(0,2)", "(0,2)", "BEN_CDI_NIR"),
        (null, null, 0L, "NA", "NA", "", "", "BEN_DTE_INS"),
        ("01/01/2006", "25/01/2006", 2L, "NA", "NA", "(01/01/2006,1)", "(01/01/2006,1)", "BEN_DTE_MAJ"),
        ("1969", "1975", 2L, "1972.0", "3944", "(1969,1)", "(1969,1)", "BEN_NAI_ANN"),
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

    When("The column names are not specified")
    Then("newDescribe should compute statistics on all the columns of the datframe")
    val result = srcDF.customDescribe()

    assert(result.collect().toSet == expectedResult.collect().toSet)

    val expectedResultColumns = {
      Seq(
        ("25/01/2008", "25/01/2008", 1L, "NA", "NA", "(25/01/2008,1)", "(25/01/2008,1)", "BEN_DCD_DTE"),
        ("Patient_01", "Patient_02", 2L, "NA", "NA", "(Patient_01,1)", "(Patient_01,1)", "NUM_ENQ")
      ).toDF("Min", "Max", "Count", "Avg", "Sum", "MaxOccur", "MinOccur", "ColName")
    }

    When("The column names are specified")
    val cols = Array("BEN_DCD_DTE", "NUM_ENQ")

    Then("Statistics should be computed only on the specified columns without exception")
    val resultColumns = srcDF.customDescribe(cols: _*) //.persist
    assert(resultColumns.collect().toSet == expectedResultColumns.collect().toSet)


    When("There is an invalid column in the list")
    val invalidCols = Array("NUM_ENQ", "INVALID_COLUMN")

    Then("An exception should be thrown")
    val thrown = intercept[java.lang.IllegalArgumentException] {
      srcDF.customDescribe(invalidCols: _*).count
    }

    assert(thrown.getMessage.matches("Field \"[^\"]*\" does not exist."))
  }

  test("Implicit class function countUnexpectedValues && countExpectedValues") {

    import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._

    // Given scrDF
    When("Applying on a String column")
    Then("The function should still work correctly")
    assert(srcDF.countUnexpectedValues("ORG_CLE_NEW", Set("CODE1234")).first().get(0) == 0)
    assert(srcDF.countExpectedValues("ORG_CLE_NEW", Set("CODE1234")).first().get(0) == 2)

    // Given a DF with Null values
    val testDF_withNull = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975)).toDF("BEN_DTE_INS")
    val combinedDF_withNull = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull)

    When("Applying on a Integer column")
    Then("The function should work correctly")
    assert(combinedDF_withNull.countUnexpectedValues("BEN_DTE_INS", (1950 to 1960).toSet).first().get(0) == 5)
    assert(combinedDF_withNull.countExpectedValues("BEN_DTE_INS", (1950 to 1960).toSet).first().get(0) == 1)

    When("Applying on a Integer column expecting double value")
    Then("The function should still work correctly")
    assert(combinedDF_withNull.countUnexpectedValues("BEN_DTE_INS", Set(1970.0)).first().get(0) == 6)
    assert(combinedDF_withNull.countExpectedValues("BEN_DTE_INS", Set(1970.0)).first().get(0) == 0)

    When("Applying on a Double column")
    Then("The function should still work correctly")
    assert(combinedDF_withNull.select($"BEN_DTE_INS" cast("double")).countUnexpectedValues("BEN_DTE_INS", Set(1970.0)).first().get(0) == 3)
    assert(combinedDF_withNull.select($"BEN_DTE_INS" cast("double")).countExpectedValues("BEN_DTE_INS", Set(1970.0)).first().get(0) == 3)

    When("Applying on a on Integer column with null Values")
    Then("The function should still work correctly")
    assert(combinedDF_withNull.countUnexpectedValues("BEN_DTE_INS", (1970 to 1975).toSet).first().get(0) == 1)
    assert(combinedDF_withNull.countExpectedValues("BEN_DTE_INS", (1970 to 1975).toSet).first().get(0) == 5)
  }

}
