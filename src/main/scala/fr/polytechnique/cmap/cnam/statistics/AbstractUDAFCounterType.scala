package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructType}

/**
  * Created by sathiya on 09/08/16.
  */

/**
  * This is an abstract class to implement UDAFs that need to do some sort of counting
  * on the column values. This class is equipped with necessary function, the implementing
  * class just has to override the update function of this class with proper logic.
  * The input column type is String and the output column type is Long.
  */
abstract class AbstractUDAFCounterType
  extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = new StructType().add("input", StringType)

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  override def bufferSchema: StructType = {
    new StructType().add("countInter", LongType)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  override def evaluate(buffer: Row): Any = buffer.getLong(0)

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true
}
