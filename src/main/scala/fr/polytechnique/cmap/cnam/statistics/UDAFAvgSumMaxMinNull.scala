package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructType}

/**
  * Created by sathiya on 26/07/16.
  */

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
    // During merge function, there could be a new chunck with default values (-1L)
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
