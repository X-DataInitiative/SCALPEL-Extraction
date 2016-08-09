package fr.polytechnique.cmap.cnam.statistics

import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer

/**
  * Created by sathiya on 26/07/16.
  */

/**
  * This UDAF takes a list of values in the constructor and counts the number of (column) values
  * that are <i>in</i> the list.
  *
  * @param expectedValues List of expected values
  * @tparam T Type of the given column.
  */
class UDAFExpectedValues[T: ClassTag](expectedValues: Set[T])
  extends AbstractUDAFCounterType {

  val expectedStrings = expectedValues.map(_.toString)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    if (expectedStrings.contains(input.getString(0))) {
      buffer(0) = buffer.getLong(0) + 1
    }
  }
}