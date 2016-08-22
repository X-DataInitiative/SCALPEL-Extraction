package fr.polytechnique.cmap.cnam

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

import scala.reflect.ClassTag
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._ // import org.apache.spark.sql.types.{DataType, LongType, StringType, StructType, NumericType}

/**
  * Created by sathiya on 28/06/16.
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

/**
  * This case class represents some of the available agg functions that can reused while computing output dataframe.
  * @param Min
  * @param Max
  * @param Count
  * @param Avg
  * @param Sum
  * @param ColName
  */
case class ReusableAggFunctions(Min: String, Max: String, Count: Long, Avg: String, Sum: String, ColName: String)

object Statistics {

  /**
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

  /**
    * This method takes a dataframe and a column name as input and computes median for the given column name.
    *
    * This method filters out the empty columns, sort the values by ascending order and convet the df to rdd.
    * With the help of the zipWithIndex method, the median is calculated:
    * val median = (df.count %2 == 0) ? (average of the two middle values) else (value at the middle position)
    *
    * @param srcDF
    * @param colName Name of the column on which the median has to be computed.
    * @return
    */
  def findMedian(srcDF: DataFrame, colName: String): Double = {

    // 'filter(! srcDF(colName).isin(""))' is used to remove empty columns.
    // If we are switching to scala 2.11, we can use spar_csv 2.11 and consider empty columns as null
    val sortedAsRdd = srcDF.select(colName).filter(!srcDF(colName).isin("")).sort(asc(colName)).map(x => x.get(0))

    val sorted_ValueAskey = sortedAsRdd.zipWithIndex().map(_.swap) //    {  case (v, idx) => (idx, v) }

    val count = sorted_ValueAskey.count()

    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      // [Quick fix, this may not be the cleaner way] String can be casted to any other types,
      // so first cast the type to string and then to needed double.
      (sorted_ValueAskey.lookup(l).head.toString.toDouble + sorted_ValueAskey.lookup(r).head.toString.toDouble) / 2
    } else sorted_ValueAskey.lookup(count / 2).head.toString.toDouble

    median
  }

  /**
    * This method does groupBy on a given column name of a df and counts the number of occurrence of each value.
    *
    * @param srcDF
    * @param colName
    * @return a dataframe with the count as the first column and the actual value as the second column.
    */
  def mapNbOccurrencesToColValues(srcDF: DataFrame, colName: String): RDD[(Long, Any)] = {

    val mappedrdd = srcDF.select(colName).filter(!srcDF(colName).isin("")).groupBy(colName).count().map(x => (x.getLong(1), x.get(0)))

    mappedrdd
  }

  /**
    * The method sorts (descending) the number of occurrence of each value in a given column.
    *
    * @param srcDF
    * @param colName
    * @param nb number of top most occurrence values to return (by default 1).
    * @return first nb rows in the sorted dataframe.
    */
  def findMostOccurrenceValues(srcDF: DataFrame, colName: String, nb: Int = 1): Iterable[(Any, Long)] = {

    val mappedRDD = mapNbOccurrencesToColValues(srcDF, colName).sortByKey(false) // false => sort descending (Most Occurrences)

    mappedRDD.take(nb).map(_.swap)
  }

  /**
    * Same as the previous method, but instead of sorting descending, this will sort ascending
    *
    * @param srcDF
    * @param colName
    * @param nb number of <i>least</i> occurrence values to return (by default 1).
    * @return
    */
  def findLeastOccurrenceValues(srcDF: DataFrame, colName: String, nb: Int = 1): Iterable[(Any, Long)] = {

    val mappedRDD = mapNbOccurrencesToColValues(srcDF, colName).sortByKey(true) // true => sort ascending (Least Occurrences)

    mappedRDD.take(nb).map(_.swap)
  }

  /**
    * This UDAF will compute Count, Sum, Avg, Min, Max and Null in a single pass.
    * However, this method is still under experimentation.
    */
  class UDAFAvgSumMaxMinNull() extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = new StructType().add("doubleInput", LongType)

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
      buffer(2) = -1L
      buffer(3) = -1L
      buffer(4) = 0L
    }

    override def bufferSchema: StructType = {
      new StructType().add("sumInput", LongType)
        .add("countInter", LongType)
        .add("maxField", LongType)
        .add("minField", LongType)
        .add("NULLField", LongType)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        val inputColValue = input.getLong(0)
        buffer(0) = buffer.getLong(0) + inputColValue
        buffer(1) = buffer.getLong(1) + 1
        // buffer(1) is already incremented by 1 in the previous step.
        buffer(2) = if (buffer(1) == 1L) inputColValue
        else {
          if (inputColValue > buffer.getLong(2)) inputColValue else buffer.getLong(2)
        }
        buffer(3) = if (buffer(1) == 1L) inputColValue
        else {
          if (inputColValue < buffer.getLong(3)) inputColValue else buffer.getLong(3)
        }
      }
      else
        buffer(4) = buffer.getLong(4) + 1

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      // we should keep these 2 computation before we compute count: buffer1(1)
      buffer1(2) = if (!(buffer1.getLong(1) == 0L || buffer2.getLong(1) == 0L)) {
        if (buffer2.getLong(2) > buffer1.getLong(2)) buffer2.getLong(2) else buffer1.getLong(2)
      } else {
        if (buffer1.getLong(1) == 0L) buffer2.getLong(2) else buffer1.getLong(2)
      }
      buffer1(3) = if (!(buffer1.getLong(1) == 0L || buffer2.getLong(1) == 0L)) {
        if (buffer2.getLong(3) < buffer1.getLong(3)) buffer2.getLong(3) else buffer1.getLong(3)
      } else {
        if (buffer1.getLong(1) == 0L) buffer2.getLong(3) else buffer1.getLong(3)
      }

      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

      buffer1(4) = buffer1.getLong(4) + buffer2.getLong(4)
    }

    override def evaluate(buffer: Row): Any = {
      (buffer.getLong(0) / buffer.getLong(1).toDouble,
        buffer.getLong(1),
        buffer.getLong(2),
        buffer.getLong(3),
        buffer.getLong(4)
        ).toString()
    }

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

  }

  /**
    * This UDAF takes a list of values in the constructor and counts the number of (column) values
    * that are <i>not</i> in the list.
    * The input schema for the UDAF is LongType.
    *
    * @param expectedValues List of expected values
    * @tparam T Type of the given column.
    */
  class CustomAggUnExpectedValues[T: ClassTag](expectedValues: Set[T]) extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = new StructType().add("input", LongType) //TODO: We may need to change the type to String.

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
    }

    override def bufferSchema: StructType = {
      new StructType().add("countInter", LongType)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

      if (!expectedValues.contains(input.getAs[T](0))) {
        buffer(0) = buffer.getLong(0) + 1
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    }

    override def evaluate(buffer: Row): Any = {
      (buffer.getLong(0)
        ).toString()
    }

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true
  }

  /**
    * Same as the previous method, but instead of counting number of (column) values that are not in the list,
    * this UDAF counts number of values that are <i>in</i> the list.
    *
    * @param expectedValues
    * @tparam T type of the given column.
    */
  class CustomAggExpectedValues[T: ClassTag](expectedValues: Set[T]) extends CustomAggUnExpectedValues(expectedValues) {

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

      if (expectedValues.contains(input.getAs(0))) {
        buffer(0) = buffer.getLong(0) + 1
      }
    }
  }

  implicit class CustomStatistics(val srcDF: DataFrame) {

    val sc = getSparkContext()
    val sqlContext = srcDF.sqlContext

    import sqlContext.implicits._

    def newDescribe(inputColumns: String*): DataFrame = {
      val src_columns = if (inputColumns.isEmpty) srcDF.columns.toList else inputColumns.toList

      val colSize = src_columns.size

      val schemaCusStat = ScalaReflection.schemaFor[ReusableAggFunctions].dataType.asInstanceOf[StructType]

      val schemaDataTypes = srcDF.schema

      var outputDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaCusStat)

      var maxOccur = Map[String, String]()
      var minOccur = Map[String, String]()

      for (i <- 0 to colSize - 1) {
        val colName: String = src_columns(i)

        if (schemaDataTypes.apply(colName).dataType.isInstanceOf[NumericType])
          outputDF = outputDF.unionAll(srcDF.select(colName).agg(min(colName), max(colName), count(colName), avg(colName), sum(colName)).withColumn("ColName", lit(colName)))
        else
          outputDF = outputDF.unionAll(srcDF.select(colName).agg(min(colName), max(colName), count(colName)).withColumn("Avg", lit("NA")).withColumn("Sum", lit("NA")).withColumn("ColName", lit(colName)))
      }

      for (i <- 0 to colSize - 1) {
        val colName: String = src_columns(i)
        maxOccur += (colName -> findMostOccurrenceValues(srcDF, colName).mkString)
        minOccur += (colName -> findLeastOccurrenceValues(srcDF, colName).mkString)
      }

      return outputDF.map((R:Row) => CaseDescribeFunctionColumns(R.getString(0),
                                                          R.getString(1),
                                                          R.getLong(2),
                                                          R.getString(3),
                                                          R.getString(4),
                                                          maxOccur.get(R.getString(5)).get,
                                                          minOccur.get(R.getString(5)).get,
                                                          R.getString(5)
                                                          )).toDF()
    }

    def computeUDAF(udafObject: UserDefinedAggregateFunction, metricName_output: String, inputColumns: Seq[String]): DataFrame = {
      val srcDfColumns = if (inputColumns.isEmpty) srcDF.columns.toList else inputColumns.toList

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

    // DELETE COMMENTED CODE LATER.
//    def countNullValues(inputColumns: String*): DataFrame = {
//
//      val nbNullVal = new UDAFNbNull()
//      val metricName_output = "Count_Null"
//      return computeUDAF(nbNullVal, metricName_output, inputColumns)
//    }

//    def countUDAFAvgSumMaxMinNull(inputColumns: String*): DataFrame = {
//
//      val allAtOnce = new UDAFAvgSumMaxMinNull()
//      val metricName_output = "AvgSumMaxMinNull"
//      return computeUDAF(allAtOnce, metricName_output, inputColumns).map(...).toDF(...Schema...)
//    }

//    def countUnexpectedValues[T: ClassTag](expectedValues: Set[T], inputColumns: String*): DataFrame = {
//      val src_columns = if (inputColumns.isEmpty) srcDF.columns.toList else inputColumns.toList
//
//      val colSize = src_columns.size
//
//      val nbUnexpectedVals = new CustomAggUnExpectedValues(expectedValues)
//
//      val schema_unexpecVal = ScalaReflection.schemaFor[NbUnexpectedValues].dataType.asInstanceOf[StructType]
//
//      var unexpecVal = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_unexpecVal)
//
//      for (i <- 0 to colSize - 1) {
//        val colName: String = src_columns(i)
//
//        unexpecVal = unexpecVal.unionAll(srcDF.select(colName).agg(nbUnexpectedVals(srcDF(colName))).withColumn("ColName", lit(colName)))
//      }
//
//      return unexpecVal
//    }
  }


  /**     DELETE COMMENTED CODE LATER.
    * This function is deprecated. As this function can be replaced by:
    *  df.agg(sum(when($"colName".isNull || $"colName" === "",1).otherwise(0))) show
    *
    * This UDAF computes the number of null/empty values in a given column of a dataframe.
    */
  //  class UDAFNbNull() extends UserDefinedAggregateFunction {
  //
  //    override def inputSchema: StructType = new StructType().add("InputInput", StringType) // We should not use numeric type as Input Schema, else the date values will be converted to Integer.
  //
  //    override def initialize(buffer: MutableAggregationBuffer): Unit = {
  //      buffer(0) = 0L
  //    }
  //
  //    override def bufferSchema: StructType = {
  //      new StructType().add("nullCount", LongType)
  //    }
  //
  //    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
  //      if (input.isNullAt(0) || input.get(0).equals("")) {
  //        buffer(0) = buffer.getLong(0) + 1
  //      }
  //      else {}
  //    }
  //
  //    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
  //      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  //    }
  //
  //    override def evaluate(buffer: Row): Any = {
  //      buffer.getLong(0)
  //    }
  //
  //    override def dataType: DataType = LongType
  //
  //    override def deterministic: Boolean = true
  //
  //    /* A small try to mix the Malformed class name Exception */
  //    //    override def toString: String = {
  //    //      //      s"""${udaf.name}(${children.mkString(",")})"""
  //    //      s"""${"UDAFNbNull"}(${inputSchema.fieldNames.mkString(",")})"""
  //    //    }
  //  }
}