package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag // import org.apache.spark.sql.types.{DataType, LongType, StringType, StructType, NumericType}

/**
  * Created by sathiya on 26/07/16.
  */

/**
  * This case class represents needed (statistics) columns for the output dataframe.
  *
  * @param Min
  * @param Max
  * @param Count
  * @param Avg
  * @param Sum
  * @param MaxOccur
  * @param MinOccur
  * @param ColName
  */
case class CaseDescribeFunctionColumns(Min: String, Max: String, Count: Long, Avg: String, Sum: String, MaxOccur: String, MinOccur: String, ColName: String)

object CustomStatistics {

  /** WE MAY NEED TO PUT THIS FUNCTION INSIDE UTILITIES/ MAIN LATER.
    * Only one SparkContext can exists for a given application.
    * This method helps not to recreate a new Spark Context if one already exists.
    *
    * @return SparkContext
    */
  def getSparkContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("SampleTest").setMaster("local") //.setMaster("mesos://localhost:5050") // mesos://localhost:1234 spark://ec2-52-51-45-139.eu-west-1.compute.amazonaws.com:7077
    val sc = SparkContext.getOrCreate(sparkConf)
    sc
  }

  implicit class Statistics(val srcDF: DataFrame) {

    val sc = getSparkContext()
    val sqlContext = srcDF.sqlContext

    import sqlContext.implicits._

    def newDescribe(inputColumns: String*): DataFrame = {
      val src_columns: List[String] = if (inputColumns.isEmpty) srcDF.columns.toList else inputColumns.toList

      val colSize = src_columns.size

      val schemaDataTypes = srcDF.schema

//      val schemaCusStat = ScalaReflection.schemaFor[CaseReusableAggFunctions].dataType.asInstanceOf[StructType]
//
//      var outputDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaCusStat)
//
//      for (i <- 0 to colSize - 1) {
//        val colName: String = src_columns(i)
//
//        if (schemaDataTypes.apply(colName).dataType.isInstanceOf[NumericType])
//          outputDF = outputDF.unionAll(srcDF.select(colName).agg(min(colName), max(colName), count(colName), sum(when(srcDF(colName).isNull || srcDF(colName) === "",1).otherwise(0)), avg(colName), sum(colName)).withColumn("ColName", lit(colName))) //  || srcDF(colName) === ""
//        else
//          outputDF = outputDF.unionAll(srcDF.select(colName).agg(min(colName), max(colName), count(colName), sum(when(srcDF(colName).isNull || srcDF(colName) === "",1).otherwise(0))).withColumn("Avg", lit("NA")).withColumn("Sum", lit("NA")).withColumn("ColName", lit(colName)))
//      }

      val outputDF = src_columns.map{
        case colName if schemaDataTypes.apply(colName).dataType.isInstanceOf[NumericType] =>
          srcDF.select(colName).filter(!col(colName).isin("")).agg(min(colName),
                                                                   max(colName),
                                                                   count(colName), //sum(when(srcDF(colName).isNull || srcDF(colName) === lit(""),1).otherwise(0)),
                                                                   avg(colName),
                                                                   sum(colName)
                                                                  ).withColumn("ColName", lit(colName))
        case colName =>
          srcDF.select(colName).filter(!col(colName).isin("")).agg(min(colName),
                                                                   max(colName),
                                                                   count(colName) // sum(when(srcDF(colName).isNull || srcDF(colName) === lit(""),1).otherwise(0))
                                                                  ).withColumn("Avg", lit("NA"))
                                                                   .withColumn("Sum", lit("NA"))
                                                                   .withColumn("ColName", lit(colName))
      }.reduce(_.unionAll(_))

      val maxOccur: Map[String, String] = src_columns.map(colName => (colName, StatisticsUsingMapReduce.findMostOccurrenceValues(srcDF, colName).mkString)).toMap
      val minOccur: Map[String, String] = src_columns.map(colName => (colName, StatisticsUsingMapReduce.findLeastOccurrenceValues(srcDF, colName).mkString)).toMap

      return outputDF.map((R: Row) => CaseDescribeFunctionColumns(R.getString(0),
        R.getString(1),
        R.getLong(2), // R.getLong(3),
        R.getString(3),
        R.getString(4),
        maxOccur.get(R.getString(5)).get,
        minOccur.get(R.getString(5)).get,
        R.getString(5)
      )).toDF()
    }

    /**
      * This is a centralized function to compute any UDAF.
      *
      * @param udafObject object of the UDAF that we would like to compute.
      * @param metricName_output column name for the computed metric.
      * @param inputColumns set of column names on which the UDAF should be applied.
      * @return dataframe that is composed of the computer metric and the column name
      */
    private def computeUDAF(udafObject: UserDefinedAggregateFunction, metricName_output: String, inputColumns: Seq[String]): DataFrame = {
      val srcDfColumns = if (inputColumns.isEmpty) srcDF.columns.toList
      else inputColumns.toList

      val colSize = srcDfColumns.size

      val schema_outputDF = new StructType().add(metricName_output, LongType).add("ColName", StringType)

      var outputDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_outputDF)

      for (i <- 0 to colSize - 1) {
        val colName: String = srcDfColumns(i)

        outputDF = outputDF.unionAll(srcDF.select(colName).agg(udafObject(srcDF(colName))).withColumn("ColName", lit(colName)))
      }

      return outputDF
    }

    /** This function counts number of unexpected values in a column <i> using UDAF </i>
      *
      * @param expectedValues set of expected values
      * @param inputColumns names of the input columns that have the same expected values
      * @tparam T
      * @return dataframe that is composed of the number of unexpected values on each input columns
      */
    def countUnexpectedValues_usingUDAF[T: ClassTag](expectedValues: Set[T], inputColumns: String*): DataFrame = {

      val nbUnexpectedVals = new CustomAggUnexpectedValues(expectedValues)
      val outputColTitle = "Count_Unexpected"
      return computeUDAF(nbUnexpectedVals, outputColTitle, inputColumns)
    }

    /** Same as the previous function, but counts expected values instead of unexpected values <i> using UDAF </i>
      *
      * @param expectedValues
      * @param inputColumns
      * @tparam T
      * @return
      */
    def countExpectedValues_usingUDAF[T: ClassTag](expectedValues: Set[T], inputColumns: String*): DataFrame = {

      val nbExpectedVals = new CustomAggExpectedValues(expectedValues)
      val outputColTitle = "Count_Expected"
      return computeUDAF(nbExpectedVals, outputColTitle, inputColumns)
    }

    /** This is centralized function for the following two functions that does same as the previous two, but <i>without</i> UDAF
      * After performance comparison, we can deprecate the method that is slow and can be eventually removed.
      * @param colName
      * @param expectedValues
      * @param choice
      * @tparam T
      * @return
      */
    private def countExpectedOrUnexpectedValues[T: ClassTag](colName: String, expectedValues: Set[T], choice: Boolean): DataFrame = {

      val castedExpectedValues = expectedValues.map(_.toString).toList
      val isInExpectedValues = srcDF.col(colName).cast(StringType).isin(castedExpectedValues: _*) //.cast(IntegerType)

      val columnToAggregate = if (choice) isInExpectedValues.cast(IntegerType) else not(isInExpectedValues).cast(IntegerType)

      srcDF.filter((col(colName).isNotNull) && not(col(colName).isin(""))).select(colName).agg(sum(columnToAggregate)).toDF(if(choice) "ExpectedValues" else "UnexpectedValues").withColumn("ColName", lit(colName))
    }

    def countUnexpectedValues[T: ClassTag](colName: String, expectedValues: Set[T]): DataFrame = {
      return countExpectedOrUnexpectedValues(colName, expectedValues, false)
    }

    def countExpectedValues[T: ClassTag](colName: String, expectedValues: Set[T]): DataFrame = {
      return countExpectedOrUnexpectedValues(colName, expectedValues, true)
    }
  }
}
