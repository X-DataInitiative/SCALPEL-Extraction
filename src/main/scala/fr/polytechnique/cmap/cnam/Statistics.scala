package fr.polytechnique.cmap.cnam

import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType

import scala.tools.nsc.util.HashSet

/**
  * Created by sathiya on 28/06/16.
  */
object Statistics {

  class CustomAggStatistics extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = new StructType().add("doubleInput", LongType)

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
      buffer(2) = 0L
      buffer(3) = 0L
      buffer(4) = 0L
    }

    override def bufferSchema: StructType = {
      new StructType().add("doubleInput", LongType)
      .add("countInter", LongType)
      .add("maxField", LongType)
      .add("minField", LongType)
      .add("NULLField", LongType)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if(!input.isNullAt(0)){
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
        buffer(2) = if (input.getLong(0) > buffer.getLong(2)) input.getLong(0) else buffer.getLong(2)
        buffer(3) = if (input.getLong(0) < buffer.getLong(3)) input.getLong(0) else buffer.getLong(3)
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

  class CustomAggUnExpectedValues[T: ClassTag](expectedValues: HashSet[T]) extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = new StructType().add("input", LongType)

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
    }

    override def bufferSchema: StructType = {
      new StructType().add("countInter", LongType)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

      if(! expectedValues.contains(input.getAs(0)))
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

  implicit class Statistic(src: DataFrame){

    val customAggStatistics = new CustomAggStatistics()

    def describe() = {
      val src_columns = src.columns

      val colSize = src_columns.size

      val statistics: Array[Array[Row]] = new Array(colSize)

      for(i <- 0 to colSize - 1 )
        {
          val colName: String = src_columns(i)
          statistics(i) = src.select(colName).agg(customAggStatistics(src(colName))).collect()
        }

      for(i <- 0 to colSize - 1 )
      {
        println( src_columns(i) + "[Avg, Count, Max, Min, #NULL] : " + statistics(i).to)
      }

    }

  }

}
