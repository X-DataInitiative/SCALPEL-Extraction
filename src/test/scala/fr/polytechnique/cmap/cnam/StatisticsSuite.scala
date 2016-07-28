package fr.polytechnique.cmap.cnam

import fr.polytechnique.cmap.cnam.statistics.{CustomAggExpectedValues, CustomAggUnexpectedValues}
import org.apache.spark.sql.{Column, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, GivenWhenThen}
import org.apache.spark.sql.types._

/**
  * Created by sathiya on 11/07/16.
  */
class StatisticsSuite extends FunSuite with GivenWhenThen {

  val sparkConf = new SparkConf().setAppName("SampleTest").setMaster("local")
  val sc = SparkContext.getOrCreate(sparkConf)
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

  test("Find Max/Min Occurrences") {

    When("I find max/min occurrence value in a column")
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_NAI_ANN")
    val combinedDF = srcDF.select("BEN_NAI_ANN").unionAll(testDF) //.select($"BEN_NAI_ANN")

    Then("I should get the right output")
    assert(Statistics.findMostOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2) == Seq((1960,1), (1969,1)))

    When("The column type is Integer")
    val combinedDF_integer = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN" cast "integer")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_integer, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF_integer, "BEN_NAI_ANN", 2) == Seq((1960,1), (1969,1)))

    When("The column type is Double")
    val combinedDF_double = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN" cast "double")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_double, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF_double, "BEN_NAI_ANN", 2) == Seq((1960,1), (1969,1)))

    When("The column type is Long")
    val combinedDF_long = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN" cast "long")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_long, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF_long, "BEN_NAI_ANN", 2) == Seq((1960,1), (1969,1)))

    When("There is null Values in the colum ")
    val testDF_withNull = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975)).toDF("BEN_DTE_INS")
    val combinedDF_withNull = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull) //.select($"BEN_DTE_INS")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_withNull, "BEN_DTE_INS", 2).toList == Seq(("1970", 3), ("1975", 2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF_withNull, "BEN_DTE_INS", 2).toList == Seq(("1960", 1), ("1975", 2)))

  }

  test("UDAF CustomAggUnexpectedValues && CustomAggExpectedValues") {

    When("We count unexpected and expected values on Integer column")
    val expectedValues = (0 to 1).toSet //.map(_.asInstanceOf[String])
    val udafUnexpected = new CustomAggUnexpectedValues(expectedValues)
    val udafExpected = new CustomAggExpectedValues(expectedValues)
    Then("The function should work correctly")
    assert(srcDF.agg(udafUnexpected(srcDF("BEN_SEX_COD"))).first().get(0) == 1)
    assert(srcDF.agg(udafExpected(srcDF("BEN_SEX_COD"))).first().get(0) == 1)

    When("We count unexpected and expected values on String column")
    val udafUnexpected_string = new CustomAggUnexpectedValues(Set("CODE1234"))
    val udafExpected_string = new CustomAggExpectedValues(Set("CODE1234"))
    Then("The function should still work correctly")
    assert(srcDF.agg(udafUnexpected_string(srcDF("ORG_CLE_NEW"))).first().get(0) == 0)
    assert(srcDF.agg(udafExpected_string(srcDF("ORG_CLE_NEW"))).first().get(0) == 2)

    When("We count unexpected and expected values on Integer column in different format")
    val udafUnexpected_format = new CustomAggUnexpectedValues(Set(0))
    val udafExpected_format = new CustomAggExpectedValues(Set(0))
    Then("The function should still work correctly")
    assert(srcDF.agg(udafUnexpected_format(srcDF("BEN_CDI_NIR"))).first().get(0) == 0)
    assert(srcDF.agg(udafExpected_format(srcDF("BEN_CDI_NIR"))).first().get(0) == 2)

    When("We count unexpected and expected values on Integer column with null Values")
    val testDF_withNull = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975)).toDF("BEN_DTE_INS")
    val combinedDF_withNull = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull)
    val udafUnexpected_null = new CustomAggUnexpectedValues((1970 to 1975).toSet)
    val udafExpected_null = new CustomAggExpectedValues((1970 to 1975).toSet)
    Then("The function should still work correctly")
    assert(combinedDF_withNull.agg(udafUnexpected_null($"BEN_DTE_INS")).first().get(0) == 1)
    assert(combinedDF_withNull.agg(udafExpected_null($"BEN_DTE_INS")).first().get(0) == 5)

    When("We count unexpected and expected values on Double column")
    val testDF_double = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_DTE_INS").select($"BEN_DTE_INS" cast "double")
    val udafUnexpected_double = new CustomAggUnexpectedValues(Set(1970.0))
    val udafExpected_double = new CustomAggExpectedValues(Set(1970.0))
    Then("The function should still work correctly")
    assert(testDF_double.agg(udafUnexpected_double($"BEN_DTE_INS")).first().get(0) == 2)
    assert(testDF_double.agg(udafExpected_double($"BEN_DTE_INS")).first().get(0) == 3)

    When("We count unexpected and expected values on Integer column expecting values in double format")
    val testDF_int_double = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_DTE_INS")//.select($"BEN_DTE_INS" cast "double")
    Then("The function should still work correctly")
    assert(testDF_int_double.agg(udafUnexpected_double($"BEN_DTE_INS")).first().get(0) == 5)
    assert(testDF_int_double.agg(udafExpected_double($"BEN_DTE_INS")).first().get(0) == 0)
  }

  test("Implicit class function countUnexpectedValues && countExpectedValues") {

    import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._

    When("We count unexpected and expected values on Integer column")
    val expectedValues = (0 to 1).toSet //.map(_.asInstanceOf[String])

    Then("The function should work correctly")
    assert(srcDF.countUnexpectedValues("BEN_SEX_COD", expectedValues).first().get(0) == 1)
    assert(srcDF.countExpectedValues("BEN_SEX_COD", expectedValues).first().get(0) == 1)

    When("We count unexpected and expected values on String column")
    Then("The function should still work correctly")
    assert(srcDF.countUnexpectedValues("ORG_CLE_NEW", Set("CODE1234")).first().get(0) == 0)
    assert(srcDF.countExpectedValues("ORG_CLE_NEW", Set("CODE1234")).first().get(0) == 2)

    When("We count unexpected and expected values on Integer column in different format")
    Then("The function should still work correctly")
    assert(srcDF.countUnexpectedValues("BEN_CDI_NIR", Set(0)).first().get(0) == 0)
    assert(srcDF.countExpectedValues("BEN_CDI_NIR", Set(0)).first().get(0) == 2)

    When("We count unexpected and expected values on Integer column with null Values")
    val testDF_withNull = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975)).toDF("BEN_DTE_INS")
    val combinedDF_withNull = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull)
    Then("The function should still work correctly")
    assert(combinedDF_withNull.countUnexpectedValues("BEN_DTE_INS", (1970 to 1975).toSet).first().get(0) == 1)
    assert(combinedDF_withNull.countExpectedValues("BEN_DTE_INS", (1970 to 1975).toSet).first().get(0) == 5)

    When("We count unexpected and expected values on Double column")
    val testDF_double = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_DTE_INS").select($"BEN_DTE_INS" cast "double")
    Then("The function should still work correctly")
    assert(testDF_double.countUnexpectedValues("BEN_DTE_INS", Set(1970.0)).first().get(0) == 2)
    assert(testDF_double.countExpectedValues("BEN_DTE_INS", Set(1970.0)).first().get(0) == 3)

    When("We count unexpected and expected values on Integer column expecting values in double format")
    val testDF_int_double = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_DTE_INS")//.select($"BEN_DTE_INS" cast "double")
    Then("The function should still work correctly")
    assert(testDF_int_double.countUnexpectedValues("BEN_DTE_INS", Set(1970.0)).first().get(0) == 5)
    assert(testDF_int_double.countExpectedValues("BEN_DTE_INS", Set(1970.0)).first().get(0) == 0)
  }

  test("Test for Describe Function") {

    import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._

    When("The column names are not specified")
    Then("newDescribe should compute statistics on all the columns of the datframe")
    val result = srcDF.newDescribe().persist
    assert(result.count == srcDF.columns.size)
    assert(result.select($"ColName").collect.map(_.getString(0)).sorted.toSeq == srcDF.columns.sorted.toSeq)
    assert(result.where($"ColName" === "BEN_CDI_NIR").first.toSeq == Seq("0", "0", 2, "0.0", "0", "(0,2)", "(0,2)", "BEN_CDI_NIR"))
    assert(result.where($"ColName" === "BEN_DCD_AME").first.toSeq == Seq("101", "200801", 2, "100451.0", "200902", "(101,1)", "(101,1)", "BEN_DCD_AME"))
    assert(result.where($"ColName" === "BEN_DCD_DTE").first.toSeq == Seq("25/01/2008", "25/01/2008", 1, "NA", "NA", "(25/01/2008,1)", "(25/01/2008,1)", "BEN_DCD_DTE"))
    assert(result.where($"ColName" === "BEN_DTE_INS").first.toSeq == Seq(null, null, 0, "NA", "NA","","", "BEN_DTE_INS"))
    assert(result.where($"ColName" === "BEN_DTE_MAJ").first.toSeq == Seq("01/01/2006", "25/01/2006", 2, "NA", "NA", "(01/01/2006,1)", "(01/01/2006,1)", "BEN_DTE_MAJ"))
    assert(result.where($"ColName" === "BEN_NAI_ANN").first.toSeq == Seq("1969", "1975", 2, "1972.0", "3944", "(1969,1)", "(1969,1)", "BEN_NAI_ANN"))
    assert(result.where($"ColName" === "BEN_NAI_MOI").first.toSeq == Seq("1", "10", 2, "5.5", "11", "(1,1)", "(1,1)", "BEN_NAI_MOI"))
    assert(result.where($"ColName" === "BEN_RES_COM").first.toSeq == Seq("24", "114", 2, "69.0", "138", "(114,1)", "(114,1)", "BEN_RES_COM"))
    assert(result.where($"ColName" === "BEN_RES_DPT").first.toSeq == Seq("75", "92", 2, "83.5", "167", "(75,1)", "(75,1)", "BEN_RES_DPT"))
    assert(result.where($"ColName" === "BEN_RNG_GEM").first.toSeq == Seq("1", "1", 2, "1.0", "2", "(1,2)", "(1,2)", "BEN_RNG_GEM"))
    assert(result.where($"ColName" === "BEN_SEX_COD").first.toSeq == Seq("1", "2", 2, "1.5", "3", "(1,1)", "(1,1)", "BEN_SEX_COD"))
    assert(result.where($"ColName" === "BEN_TOP_CNS").first.toSeq == Seq("1", "1", 2, "1.0", "2", "(1,2)", "(1,2)", "BEN_TOP_CNS"))
    assert(result.where($"ColName" === "MAX_TRT_DTD").first.toSeq == Seq("07/03/2008", "07/03/2008", 1, "NA", "NA", "(07/03/2008,1)", "(07/03/2008,1)", "MAX_TRT_DTD"))
    assert(result.where($"ColName" === "NUM_ENQ").first.toSeq     == Seq("Patient_01", "Patient_02", 2, "NA", "NA", "(Patient_01,1)", "(Patient_01,1)", "NUM_ENQ"))
    assert(result.where($"ColName" === "ORG_AFF_BEN").first.toSeq == Seq("CODE1234", "CODE1234", 2, "NA", "NA", "(CODE1234,2)", "(CODE1234,2)", "ORG_AFF_BEN"))
    assert(result.where($"ColName" === "ORG_CLE_NEW").first.toSeq == Seq("CODE1234", "CODE1234", 2, "NA", "NA", "(CODE1234,2)", "(CODE1234,2)", "ORG_CLE_NEW"))


    When("There the column names are specified")
    val cols = Array("BEN_DCD_DTE", "NUM_ENQ")

    Then("newDescribe should compute statistics on the specified columns without exception")
    val result_specified = srcDF.newDescribe(cols: _*).persist
    assert(result_specified.count == cols.size)
    assert(result_specified.select($"ColName").collect.map(_.getString(0)).sorted.toSeq == cols.sorted.toSeq)
    assert(result_specified.where($"ColName" === "BEN_DCD_DTE").first.toSeq == Seq("25/01/2008", "25/01/2008", 1, "NA", "NA", "(25/01/2008,1)", "(25/01/2008,1)", "BEN_DCD_DTE"))
    assert(result_specified.where($"ColName" === "NUM_ENQ").first.toSeq     == Seq("Patient_01", "Patient_02", 2, "NA", "NA", "(Patient_01,1)", "(Patient_01,1)", "NUM_ENQ"))

    When("There is an invalid column in the list")
    val invalidCols = Array("NUM_ENQ", "INVALID_COLUMN")

    Then("An exception should be thrown")
    val thrown = intercept[java.lang.IllegalArgumentException] {
      srcDF.newDescribe(invalidCols: _*).count
    }
    assert(thrown.getMessage.matches("Field \"[^\"]*\" does not exist."))

  }


  /**
    * We have decided not to use median for the moment as it takes too much time to compute.
    * In future if we wanted to compute median, we should go for the native approximation algorithm
    * instaed of the map reduce (accurate) one.
    */
  //  test("Find Median") {
  //
  //    When("I find Median with even number of Rows")
  //    val medianOutput_even = Statistics.findMedian(srcDF, "BEN_NAI_ANN")
  //
  //    Then("I should get the average of two middle values as output")
  //    assert(medianOutput_even == 1972)
  //
  //    When("The column type is Integer/Double/Long [in case of even number of Rows]")
  //    val srcDF_integer = srcDF.select($"BEN_NAI_ANN" cast "integer")
  //    val srcDF_double = srcDF.select($"BEN_NAI_ANN" cast "double")
  //    val srcDF_long = srcDF.select($"BEN_NAI_ANN" cast "long")
  //
  //    Then("The function should work without exceptions [in case of even number of Rows]")
  //    assert(Statistics.findMedian(srcDF_integer, "BEN_NAI_ANN") == 1972)
  //    assert(Statistics.findMedian(srcDF_double, "BEN_NAI_ANN") == 1972)
  //    assert(Statistics.findMedian(srcDF_long, "BEN_NAI_ANN") == 1972)
  //
  //    When("There is null Values in the colum [in case of even number of Rows] ")
  //    val testDF_withNull_even = sc.parallelize(List(1970,1960,1920,1940)).toDF("BEN_DTE_INS")
  //    val combinedDF_withNull_even = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull_even).select($"BEN_DTE_INS")
  //
  //    Then("The function should work without exceptions [in case of odd number of Rows]")
  //    assert(Statistics.findMedian(combinedDF_withNull_even, "BEN_DTE_INS") == 1950)
  //
  //    When("The column type is Integer/Double/Long with null values [in case of even number of Rows]")
  //    val combinedDF_withNull_even_integer = combinedDF_withNull_even.select($"BEN_DTE_INS" cast "integer")
  //    val combinedDF_withNull_even_double = combinedDF_withNull_even.select($"BEN_DTE_INS" cast "double")
  //    val combinedDF_withNull_even_long = combinedDF_withNull_even.select($"BEN_DTE_INS" cast "long")
  //
  //    Then("The function should work without exceptions [in case of even number of Rows]")
  //    assert(Statistics.findMedian(combinedDF_withNull_even_integer, "BEN_DTE_INS") == 1950)
  //    assert(Statistics.findMedian(combinedDF_withNull_even_double, "BEN_DTE_INS") == 1950)
  //    assert(Statistics.findMedian(combinedDF_withNull_even_long, "BEN_DTE_INS") == 1950)
  //
  //    When("I find Median with odd number of Rows")
  //    val testDF = sc.parallelize(List(1970)).toDF("BEN_NAI_ANN")
  //    val combinedDF = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN")
  //
  //    val medianOutput_odd = Statistics.findMedian(combinedDF, "BEN_NAI_ANN")
  //
  //    Then("I should get the middle value as output")
  //    assert(medianOutput_odd == 1970)
  //
  //    When("The column type is Integer/Double/Long [in case of odd number of Rows]")
  //    val combinedDF_integer = combinedDF.select($"BEN_NAI_ANN" cast "integer")
  //    val combinedDF_double = combinedDF.select($"BEN_NAI_ANN" cast "double")
  //    val combinedDF_long = combinedDF.select($"BEN_NAI_ANN" cast "long")
  //
  //    Then("The function should work without exceptions [in case of odd number of Rows]")
  //    assert(Statistics.findMedian(combinedDF_integer, "BEN_NAI_ANN") == 1970)
  //    assert(Statistics.findMedian(combinedDF_double, "BEN_NAI_ANN") == 1970)
  //    assert(Statistics.findMedian(combinedDF_long, "BEN_NAI_ANN") == 1970)
  //
  //
  //    When("There is null Values in the colum [in case of odd number of Rows] ")
  //    val testDF_withNull_odd = sc.parallelize(List(1970,1960,1920,1940,1990)).toDF("BEN_DTE_INS")
  //    val combinedDF_withNull_odd = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull_odd).select($"BEN_DTE_INS")
  //
  //    Then("The function should work without exceptions [in case of odd number of Rows]")
  //    assert(Statistics.findMedian(combinedDF_withNull_odd, "BEN_DTE_INS") == 1960)
  //
  //    When("The column type is Integer/Double/Long with null values [in case of odd number of Rows]")
  //    val combinedDF_withNull_odd_integer = combinedDF_withNull_odd.select($"BEN_DTE_INS" cast "integer")
  //    val combinedDF_withNull_odd_double = combinedDF_withNull_odd.select($"BEN_DTE_INS" cast "double")
  //    val combinedDF_withNull_odd_long = combinedDF_withNull_odd.select($"BEN_DTE_INS" cast "long")
  //
  //    Then("The function should work without exceptions [in case of odd number of Rows]")
  //    assert(Statistics.findMedian(combinedDF_withNull_odd_integer, "BEN_DTE_INS") == 1960)
  //    assert(Statistics.findMedian(combinedDF_withNull_odd_double, "BEN_DTE_INS") == 1960)
  //    assert(Statistics.findMedian(combinedDF_withNull_odd_long, "BEN_DTE_INS") == 1960)
  //
  //  }

  /**
    * This UDAF is deprecated.
    */
  //  test("UDAF NbNull") {
//
//    val nbNullVal = new UDAFNbNull()
//    //val all = new UDAFAvgSumMaxMinNull()
//
//    val srcDF_mini = srcDF.select("BEN_CDI_NIR", "BEN_NAI_ANN", "BEN_DTE_INS", "BEN_SEX_COD", "BEN_TOP_CNS", "BEN_DCD_AME","BEN_DCD_DTE")
//
//    val src_columns = srcDF.columns
//
//    val colSize = src_columns.size
//
//    val expectedOutput = Array(0,2,0,0,0,0,0,0,0,0,1,2,2,2,1,0)
//    val expectedOutput_srcDFMini = Array(0,2,0,0,0,0,0,0,0,0,1,2,2,2,1,0)
//    When("I find number of null values")
//    Then("I should get the expected Output")
//
//    var output = new ListBuffer[String]()
//    for(i <- 0 to srcDF_mini.columns.size - 1 ) {
//      val colName: String = srcDF_mini.columns(i) //src_columns(i)
//      output += colName + " :" +srcDF_mini.select(colName).agg(nbNullVal(srcDF_mini(colName))).first().getLong(0)
//      //assert(srcDF.select(colName).agg(nbNullVal(srcDF(colName))).first().getLong(0) == expectedOutput(i))
//    }
//    output foreach println
//  }

//  test("newDescribe on data frame") {
//
//    import Statistics._
//
////    val expectedOutput = sc.parallelize(List(0, 0, 2, 0, 0)).toDF("BEN_CDI_NIR")
////
////    val testDF_withNull = sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975)).toDF("BEN_DTE_INS")
//
//    def expectedVals[T: ClassTag] = Set[T](0,1)
//    srcDF.countExpectedValues(expectedVals) show
//  }

}

