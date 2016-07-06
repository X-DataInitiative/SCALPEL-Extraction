package fr.polytechnique.cmap.cnam

import java.util.HashMap

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.reflect.ClassTag
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType

import org.apache.spark.sql.functions._

import scalaz._
import Scalaz._
import scala.tools.nsc.util.HashSet

/**
  * Created by sathiya on 28/06/16.
  */
object Statistics {

  val sparkConf = new SparkConf().setAppName("SampleTest").setMaster("local") //.setMaster("mesos://localhost:5050") // mesos://localhost:1234 spark://ec2-52-51-45-139.eu-west-1.compute.amazonaws.com:7077
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  class CustomAggStatistics extends UserDefinedAggregateFunction {

//    var freq_colOccur = Map[Long, Int]()
//    def mapFrequency(inputColValue: Long) = {
//
//      if(freq_colOccur.contains(inputColValue))
//        freq_colOccur += (inputColValue -> (freq_colOccur.get(inputColValue).get + 1))
//      else
//        freq_colOccur += (inputColValue -> 1)
//    }

    override def inputSchema: StructType = new StructType().add("doubleInput", LongType)

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
      buffer(2) = 0L
      buffer(3) = 0L
      buffer(4) = 0L
      buffer(5) =
//      buffer(5) = Map[Long, Long]()
    }

    override def bufferSchema: StructType = {
      new StructType().add("sumInput", LongType)
      .add("countInter", LongType)
      .add("maxField", LongType)
      .add("minField", LongType)
      .add("NULLField", LongType)
//      .add("DistinctField", LongType)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if(!input.isNullAt(0)){
        val inputColValue = input.getLong(0)
        buffer(0) = buffer.getLong(0) + inputColValue
        buffer(1) = buffer.getLong(1) + 1
        buffer(2) = if (inputColValue > buffer.getLong(2)) inputColValue else buffer.getLong(2)
        buffer(3) = if (inputColValue < buffer.getLong(3)) inputColValue else buffer.getLong(3)
//        buffer(5) = mapFrequency(inputColValue)
////        var freq_colOccur = buffer.getMap[Long, Long](5)
//
//          if(freq_colOccur.contains(inputColValue))
//            freq_colOccur += (inputColValue -> (freq_colOccur.get(inputColValue).get + 1L))
//          else
//            freq_colOccur += (inputColValue -> 1L)
//
//        buffer(5) = freq_colOccur

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

//      val map1 = buffer1.getMap[Long, Long](5)
//      val map2 = buffer2.getMap(5)
//
//      buffer1(5) = map1 //|+| map2
//      buffer1(5) = map1.merged(map2)({ case ((k,v1),(_,v2)) => (k,v1+v2) })
//      map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0)) }
    }

    override def evaluate(buffer: Row): Any = {
      (buffer.getLong(0)/buffer.getLong(1).toDouble,
        buffer.getLong(1),
        buffer.getLong(2),
        buffer.getLong(3),
        buffer.getLong(4)
//        buffer.getMap(5)
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

  implicit class Statistic(src: DataFrame){

    val customAggStatistics = new CustomAggStatistics()

    def newDescribe() = {
      val src_columns = src.describe()..columns

      val colSize = src_columns.size

      val statistics: Array[Array[Row]] = new Array(colSize)

      for(i <- 0 to colSize - 1 )
        {
          val colName: String = src_columns(i)
          statistics(i) = src.select(colName).agg(customAggStatistics(src(colName))).collect()
          src.groupBy("BEN_NAI_MOI").count.agg(max(src("count")))//.groupBy("count").max("count") show
        }

      for(i <- 0 to colSize - 1 )
      {
        println( src_columns(i) + "[Avg, Count, Max, Min, #NULL] : " + statistics(i).to)
      }

    }

  }

}
