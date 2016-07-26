package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._

import scala.reflect.ClassTag
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._ // import org.apache.spark.sql.types.{DataType, LongType, StringType, StructType, NumericType}

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
case class CaseDescribeFunctionColumns(Min: String, Max: String, Count: Long, NullValues: Long, Avg: String, Sum: String, MaxOccur: String, MinOccur: String, ColName: String)

/**
  * This case class represents some of the available agg functions that can reused while computing output dataframe.
 *
  * @param Min
  * @param Max
  * @param Count
  * @param Avg
  * @param Sum
  * @param ColName
  */
case class CaseReusableAggFunctions(Min: String, Max: String, Count: Long, NullValues: Long, Avg: String, Sum: String, ColName: String)

object CustomStatistics {

  /** WE MAY NEED TO PUT THIS FUNCTION INSIDE UTILITIES LATER.
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
          srcDF.select(colName).agg(min(colName),
                                    max(colName),
                                    count(colName),
                                    sum(when(srcDF(colName).isNull || srcDF(colName) === "",1).otherwise(0)),
                                    avg(colName),
                                    sum(colName)
                                   ).withColumn("ColName", lit(colName))
        case colName =>
          srcDF.select(colName).agg(min(colName),
                                    max(colName),
                                    count(colName),
                                    sum(when(srcDF(colName).isNull || srcDF(colName) === "",1).otherwise(0))
                                    ).withColumn("Avg", lit("NA"))
                                     .withColumn("Sum", lit("NA"))
                                     .withColumn("ColName", lit(colName))
      }.reduce(_.unionAll(_))

      val maxOccur: Map[String, String] = src_columns.map(colName => (colName, StatisticsUsingMapReduce.findMostOccurrenceValues(srcDF, colName).mkString)).toMap
      val minOccur: Map[String, String] = src_columns.map(colName => (colName, StatisticsUsingMapReduce.findLeastOccurrenceValues(srcDF, colName).mkString)).toMap

      return outputDF.map((R: Row) => CaseDescribeFunctionColumns(R.getString(0),
        R.getString(1),
        R.getLong(2),
        R.getLong(3),
        R.getString(4),
        R.getString(5),
        maxOccur.get(R.getString(6)).get,
        minOccur.get(R.getString(6)).get,
        R.getString(6)
      )).toDF()
    }

    def computeUDAF(udafObject: UserDefinedAggregateFunction, metricName_output: String, inputColumns: Seq[String]):
    DataFrame = {
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

    def countUnexpectedValues[T: ClassTag](expectedValues: Set[T], inputColumns: String*): DataFrame = {

      val nbUnexpectedVals = new CustomAggUnExpectedValues(expectedValues)
      val outputColTitle = "Count_Unexpected"
      return computeUDAF(nbUnexpectedVals, outputColTitle, inputColumns)
    }

    def countExpectedValues[T: ClassTag](expectedValues: Set[T], inputColumns: String*): DataFrame = {

      val nbExpectedVals = new CustomAggExpectedValues(expectedValues)
      val outputColTitle = "Count_Expected"
      return computeUDAF(nbExpectedVals, outputColTitle, inputColumns)
    }
  }
}
