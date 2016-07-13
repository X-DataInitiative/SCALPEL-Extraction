package fr.polytechnique.cmap.cnam

import java.util.HashMap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.reflect.ClassTag
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{AnyDataType, _}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
//import scalaz._
//import Scalaz._
import scala.tools.nsc.util.HashSet

/**
  * Created by sathiya on 28/06/16.
  */
object Statistics {

  val sparkConf = new SparkConf().setAppName("SampleTest").setMaster("local") //.setMaster("mesos://localhost:5050") // mesos://localhost:1234 spark://ec2-52-51-45-139.eu-west-1.compute.amazonaws.com:7077
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  def findMedian(srcDF: DataFrame, colName: String): Double = {

    // 'filter(! srcDF(colName).isin(""))' is used to remove empty columns.
    // If we are switching to scala 2.11, we can use spar_csv 2.11 and consider empty columns as null
    val sortedAsRdd = srcDF.select(colName).filter(!srcDF(colName).isin("")).sort(asc(colName)).map(x=> x.get(0))

    val test = sortedAsRdd.collect()

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

  def mapNbOccurrencesToColValues(srcDF: DataFrame, colName: String): RDD[(Long, Any)] = {

    val mappedrdd = srcDF.select(colName).filter(!srcDF(colName).isin("")).groupBy(colName).count().map(x => (x.getLong(1), x.get(0)))

    val testValue = mappedrdd.collect()
    mappedrdd
  }

  def findMostOccurrenceValues(srcDF: DataFrame, colName: String, nb: Int = 5): Iterable[(Any, Long)] = {

    val mappedRDD = mapNbOccurrencesToColValues(srcDF, colName).sortByKey(false) // false => sort descending (Most Occurrences)

    mappedRDD.take(nb).map(_.swap)
  }

  def findLeastOccurrenceValues(srcDF: DataFrame, colName: String, nb: Int = 5): Iterable[(Any, Long)] = {

    val mappedRDD = mapNbOccurrencesToColValues(srcDF, colName).sortByKey(true) // true => sort ascending (Least Occurrences)

    mappedRDD.take(nb).map(_.swap)
  }

  class UDAFAvgSumMaxMinNull() extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = new StructType().add("doubleInput", LongType)

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
      buffer(2) = 0L
      buffer(3) = 0L
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
      if(!input.isNullAt(0)){
        val inputColValue = input.getLong(0)
        buffer(0) = buffer.getLong(0) + inputColValue
        buffer(1) = buffer.getLong(1) + 1
        buffer(2) = if (inputColValue > buffer.getLong(2)) inputColValue else buffer.getLong(2)
        buffer(3) = if (inputColValue < buffer.getLong(3)) inputColValue else buffer.getLong(3)
      }
      else
        buffer(4) = buffer.getLong(4) + 1

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
      buffer1(2) = if (buffer2.getLong(2) > buffer1.getLong(2)) buffer2.getLong(2) else buffer1.getLong(2)
      buffer1(3) = if (buffer2.getLong(3) < buffer1.getLong(3)) buffer2.getLong(3) else buffer1.getLong(3)
      buffer1(4) = buffer1.getLong(4) + buffer2.getLong(4)
    }

    override def evaluate(buffer: Row): Any = {
      (buffer.getLong(0)/buffer.getLong(1).toDouble,
        buffer.getLong(1),
        buffer.getLong(2),
        buffer.getLong(3),
        buffer.getLong(4)
        ).toString()
    }

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

  }

  class CustomAggUnExpectedValues[T: ClassTag](expectedValues: Set[T]) extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = new StructType().add("input", LongType)

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
    }

    override def bufferSchema: StructType = {
      new StructType().add("countInter", LongType)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

      if(! expectedValues.contains(input.getAs[T](0)))
      {
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

  class CustomAggExpectedValues[T: ClassTag](expectedValues: Set[T]) extends CustomAggUnExpectedValues(expectedValues){

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

      if(expectedValues.contains(input.getAs(0)))
      {
        buffer(0) = buffer.getLong(0) + 1
      }
    }
  }

  implicit class Statistic(srcDF: DataFrame){

    val avg_sum_max_min_null = new UDAFAvgSumMaxMinNull()

    def newDescribe() = {
      val src_columns = srcDF.columns

      val colSize = src_columns.size

      val statistics: Array[Array[Row]] = new Array(colSize)

      var listOfAgg = Map[String, String]()
      for(i <- 0 to colSize - 1 )
        {
          val colName: String = src_columns(i)
//          statistics(i) = srcDF.select(colName).agg(avg_sum_max_min_null(srcDF(colName))).collect()
//          val listOfAgg = Map(colName -> "min",
//            colName -> "min",
//            colName -> "max")
          statistics(i) = srcDF.agg(min(colName), max(colName), avg(colName), count(colName), sum(colName)).collect()
        }


      srcDF.groupBy("BEN_NAI_MOI").count.agg(max(srcDF("count")))//.groupBy("count").max("count") show

      for(i <- 0 to colSize - 1 )
      {
        println( src_columns(i) + "[Min, Max, Mean, Count, Sum] : " + statistics(i).to)
//        println( src_columns(i) + "[Avg, Count, Max, Min, Null] : " + statistics(i).to)
      }

    }

  }

}
