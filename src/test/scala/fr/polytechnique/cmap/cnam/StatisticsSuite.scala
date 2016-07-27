package fr.polytechnique.cmap.cnam

import fr.polytechnique.cmap.cnam.statistics.{CustomAggExpectedValues, CustomAggUnexpectedValues}
import org.apache.spark.sql.{Column, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, GivenWhenThen}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.{AnyDataType, _}

import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * Created by sathiya on 11/07/16.
  */
class StatisticsSuite extends FunSuite with GivenWhenThen {

  val sparkConf = new SparkConf().setAppName("SampleTest").setMaster("local") //.setMaster("mesos://localhost:5050") // mesos://localhost:1234 spark://ec2-52-51-45-139.eu-west-1.compute.amazonaws.com:7077
  val sc = SparkContext.getOrCreate(sparkConf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val srcFilePath: String = "src/test/resources/IR_BEN_R.csv"

  val srcDF = { sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .option("treatEmptyValuesAsNulls", "true")
    .option("nullValue", "")
    .load(srcFilePath)
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

  test("Find Max/Min Occurrences") {

    When("I find max/min occurrence value in a column")
    val testDF = sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_NAI_ANN")
    val combinedDF = srcDF.select("BEN_NAI_ANN").unionAll(testDF) //.select($"BEN_NAI_ANN")

    Then("I should get the right output")
    assert(Statistics.findMostOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))
    assert(Statistics.findLeastOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2) == Seq((1960,1), (1969,1)))

//    Then("The output should be correct irrespective of the order")
//    assert(Statistics.findMostOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2) == Set((1975,2), (1970,3)))
//    assert(Statistics.findLeastOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2) == Set((1960,1), (1969,1)))


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

//    When("The column type is Integer/Double/Long with null values")
//    val combinedDF_withNull_integer = combinedDF_withNull.select($"BEN_DTE_INS" cast "integer")
//    val combinedDF_withNull_double = combinedDF_withNull.select($"BEN_DTE_INS" cast "double")
//    val combinedDF_withNull_long = combinedDF_withNull.select($"BEN_DTE_INS" cast "long")
//
//    Then("The function should work without exceptions")
//    assert(Statistics.findMostOccurrenceValues(combinedDF_withNull_integer, "BEN_DTE_INS", 2).toSeq == Seq((1970,3), (1975,2)))
//    assert(Statistics.findMostOccurrenceValues(combinedDF_withNull_double, "BEN_DTE_INS", 2).toSeq == Seq((1970,3), (1975,2)))
//    assert(Statistics.findMostOccurrenceValues(combinedDF_withNull_long, "BEN_DTE_INS", 2).toSeq == Seq((1970,3), (1975,2)))
//
//    assert(Statistics.findLeastOccurrenceValues(combinedDF_withNull_integer, "BEN_DTE_INS", 2).toSeq == Seq((1960,1), (1975,2)))
//    assert(Statistics.findLeastOccurrenceValues(combinedDF_withNull_double, "BEN_DTE_INS", 2).toSeq == Seq((1960,1), (1975,2)))
//    assert(Statistics.findLeastOccurrenceValues(combinedDF_withNull_long, "BEN_DTE_INS", 2).toSeq == Seq((1960,1), (1975,2)))

  }

  test("UDAF CustomAggUnExpectedValues") {


    val expectedValues = (0 to 1).toSet //.map(_.asInstanceOf[String])

    val udafUnexpected = new CustomAggUnexpectedValues(expectedValues)
    assert(srcDF.agg(udafUnexpected(srcDF("BEN_SEX_COD"))).first().get(0) == 1)

    val udafUnexpected_string = new CustomAggUnexpectedValues(Set("CODE1234"))
    assert(srcDF.agg(udafUnexpected_string(srcDF("ORG_CLE_NEW"))).first().get(0) == 2)

    val udafUnexpected_double = new CustomAggUnexpectedValues(Set(0))
    assert(srcDF.agg(udafUnexpected_double(srcDF("BEN_CDI_NIR"))).first().get(0) == 2)
//    val udafUnexpected = new CustomAggExpectedValues(expectedValues)

//    println(output)
  }

  test("Implicit class function countUnexpectedValues") {


    val expectedValues = (0 to 1).toSet //.map(_.asInstanceOf[String])

    import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._
    //val udafUnexpected = new CustomAggExpectedValues(expectedValues)

    val output = {
      srcDF.select("BEN_SEX_COD").countExpectedValues("BEN_SEX_COD", expectedValues)
    } //.agg(udafUnexpected(srcDF("BEN_SEX_COD"))).first().get(0)

    println(output)
  }

  test(" Test for Describe Function") {

  }
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

