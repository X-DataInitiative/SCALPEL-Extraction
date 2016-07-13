package fr.polytechnique.cmap.cnam

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, GivenWhenThen}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.{AnyDataType, _}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by sathiya on 11/07/16.
  */
class StatisticsSuite extends FunSuite with GivenWhenThen {

//  val sparkConf = new SparkConf().setAppName("SampleTest").setMaster("local") //.setMaster("mesos://localhost:5050") // mesos://localhost:1234 spark://ec2-52-51-45-139.eu-west-1.compute.amazonaws.com:7077
//  val sc = new SparkContext(sparkConf)
//  val sqlContext = new SQLContext(sc)

  import Statistics.sqlContext.implicits._

  val srcFilePath: String = "src/test/resources/IR_BEN_R.csv"

  val srcDF = { Statistics.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .option("treatEmptyValuesAsNulls", "true")
    .option("nullValue", "null")
    .load(srcFilePath)
  }





  test("Find Median") {

    When("I find Median with even number of Rows")
    val medianOutput_even = Statistics.findMedian(srcDF, "BEN_NAI_ANN")

    Then("I should get the average of two middle values as output")
    assert(medianOutput_even == 1972)

    When("The column type is Integer/Double/Long [in case of even number of Rows]")
    val srcDF_integer = srcDF.select($"BEN_NAI_ANN" cast "integer")
    val srcDF_double = srcDF.select($"BEN_NAI_ANN" cast "double")
    val srcDF_long = srcDF.select($"BEN_NAI_ANN" cast "long")

    Then("The function should work without exceptions [in case of even number of Rows]")
    assert(Statistics.findMedian(srcDF_integer, "BEN_NAI_ANN") == 1972)
    assert(Statistics.findMedian(srcDF_double, "BEN_NAI_ANN") == 1972)
    assert(Statistics.findMedian(srcDF_long, "BEN_NAI_ANN") == 1972)

    When("There is null Values in the colum [in case of even number of Rows] ")
    val testDF_withNull_even = Statistics.sc.parallelize(List(1970,1960,1920,1940)).toDF("BEN_DTE_INS")
    val combinedDF_withNull_even = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull_even).select($"BEN_DTE_INS")

    Then("The function should work without exceptions [in case of odd number of Rows]")
    assert(Statistics.findMedian(combinedDF_withNull_even, "BEN_DTE_INS") == 1950)

    When("The column type is Integer/Double/Long with null values [in case of even number of Rows]")
    val combinedDF_withNull_even_integer = combinedDF_withNull_even.select($"BEN_DTE_INS" cast "integer")
    val combinedDF_withNull_even_double = combinedDF_withNull_even.select($"BEN_DTE_INS" cast "double")
    val combinedDF_withNull_even_long = combinedDF_withNull_even.select($"BEN_DTE_INS" cast "long")

    Then("The function should work without exceptions [in case of even number of Rows]")
    assert(Statistics.findMedian(combinedDF_withNull_even_integer, "BEN_DTE_INS") == 1950)
    assert(Statistics.findMedian(combinedDF_withNull_even_double, "BEN_DTE_INS") == 1950)
    assert(Statistics.findMedian(combinedDF_withNull_even_long, "BEN_DTE_INS") == 1950)

    When("I find Median with odd number of Rows")
    val testDF = Statistics.sc.parallelize(List(1970)).toDF("BEN_NAI_ANN")
    val combinedDF = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN")

    val medianOutput_odd = Statistics.findMedian(combinedDF, "BEN_NAI_ANN")

    Then("I should get the middle value as output")
    assert(medianOutput_odd == 1970)

    When("The column type is Integer/Double/Long [in case of odd number of Rows]")
    val combinedDF_integer = combinedDF.select($"BEN_NAI_ANN" cast "integer")
    val combinedDF_double = combinedDF.select($"BEN_NAI_ANN" cast "double")
    val combinedDF_long = combinedDF.select($"BEN_NAI_ANN" cast "long")

    Then("The function should work without exceptions [in case of odd number of Rows]")
    assert(Statistics.findMedian(combinedDF_integer, "BEN_NAI_ANN") == 1970)
    assert(Statistics.findMedian(combinedDF_double, "BEN_NAI_ANN") == 1970)
    assert(Statistics.findMedian(combinedDF_long, "BEN_NAI_ANN") == 1970)


    When("There is null Values in the colum [in case of odd number of Rows] ")
    val testDF_withNull_odd = Statistics.sc.parallelize(List(1970,1960,1920,1940,1990)).toDF("BEN_DTE_INS")
    val combinedDF_withNull_odd = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull_odd).select($"BEN_DTE_INS")

    Then("The function should work without exceptions [in case of odd number of Rows]")
    assert(Statistics.findMedian(combinedDF_withNull_odd, "BEN_DTE_INS") == 1960)

    When("The column type is Integer/Double/Long with null values [in case of odd number of Rows]")
    val combinedDF_withNull_odd_integer = combinedDF_withNull_odd.select($"BEN_DTE_INS" cast "integer")
    val combinedDF_withNull_odd_double = combinedDF_withNull_odd.select($"BEN_DTE_INS" cast "double")
    val combinedDF_withNull_odd_long = combinedDF_withNull_odd.select($"BEN_DTE_INS" cast "long")

    Then("The function should work without exceptions [in case of odd number of Rows]")
    assert(Statistics.findMedian(combinedDF_withNull_odd_integer, "BEN_DTE_INS") == 1960)
    assert(Statistics.findMedian(combinedDF_withNull_odd_double, "BEN_DTE_INS") == 1960)
    assert(Statistics.findMedian(combinedDF_withNull_odd_long, "BEN_DTE_INS") == 1960)

  }

  test("Find Max Occurrences") {

    When("I find max occurrence value in a column")
    val testDF = Statistics.sc.parallelize(List(1970, 1975, 1970, 1960, 1970)).toDF("BEN_NAI_ANN")
    val combinedDF = srcDF.select("BEN_NAI_ANN").unionAll(testDF) //.select($"BEN_NAI_ANN")

    Then("I should get the right output")
    assert(Statistics.findMostOccurrenceValues(combinedDF, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))


    When("The column type is Integer")
    val combinedDF_integer = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN" cast "integer")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_integer, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))

    When("The column type is Double")
    val combinedDF_double = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN" cast "double")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_double, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))

    When("The column type is Long")
    val combinedDF_long = srcDF.select("BEN_NAI_ANN").unionAll(testDF).select($"BEN_NAI_ANN" cast "long")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_long, "BEN_NAI_ANN", 2) == Seq((1970,3), (1975,2)))

    When("There is null Values in the colum ")
    val testDF_withNull = Statistics.sc.parallelize(List(1970, 1975, 1970, 1960, 1970, 1975)).toDF("BEN_DTE_INS")
    val combinedDF_withNull = srcDF.select("BEN_DTE_INS").unionAll(testDF_withNull) //.select($"BEN_DTE_INS")

    Then("The function should work without exceptions")
    val testOutPut = Statistics.findMostOccurrenceValues(combinedDF_withNull, "BEN_DTE_INS", 2).toList
    assert(testOutPut == Seq(("1970", 3), ("1975", 2)))

    When("The column type is Integer/Double/Long with null values")
    val combinedDF_withNull_integer = combinedDF_withNull.select($"BEN_DTE_INS" cast "integer")
    val combinedDF_withNull_double = combinedDF_withNull.select($"BEN_DTE_INS" cast "double")
    val combinedDF_withNull_long = combinedDF_withNull.select($"BEN_DTE_INS" cast "long")

    Then("The function should work without exceptions")
    assert(Statistics.findMostOccurrenceValues(combinedDF_withNull_integer, "BEN_DTE_INS", 2).toSeq == Seq((1970,3), (1975,2)))
    assert(Statistics.findMostOccurrenceValues(combinedDF_withNull_double, "BEN_DTE_INS", 2).toSeq == Seq((1970,3), (1975,2)))
    assert(Statistics.findMostOccurrenceValues(combinedDF_withNull_long, "BEN_DTE_INS", 2).toSeq == Seq((1970,3), (1975,2)))

  }
}
