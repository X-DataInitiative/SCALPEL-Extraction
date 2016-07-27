package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
  * Created by sathiya on 26/07/16.
  */

/**
  * This UDAF takes a list of values in the constructor and counts the number of (column) values
  * that are <i>not</i> in the list.
  * The input schema for the UDAF is LongType.
  *
  * @param expectedValues List of expected values
  * @tparam T Type of the given column.
  */
class CustomAggUnexpectedValues[T: ClassTag](expectedValues: Set[T]) extends UserDefinedAggregateFunction {

  val castedExpectedValues = expectedValues.map(_.toString)

  override def inputSchema: StructType = new StructType().add("input", StringType) //TODO: We may need to change the type to String.

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  override def bufferSchema: StructType = {
    new StructType().add("countInter", LongType)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    if (!castedExpectedValues.contains(input.getString(0))) { // if (!expectedValues.find( _ == input.getAs[T](0)).isDefined) {
      buffer(0) = buffer.getLong(0) + 1
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true
}