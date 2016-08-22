package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer

import scala.reflect.ClassTag

/**
  * Created by sathiya on 26/07/16.
  */

/**
  * This UDAF takes a list of values in the constructor and counts the number of (column) values
  * that are <i>not in</i> the list.
  *
  * @param expectedValues List of expected values
  * @tparam T Type of the given column.
  */
class UDAFUnexpectedValues[T: ClassTag](expectedValues: Set[T])
  extends AbstractUDAFCounterType {

  // We should not consider null/empty as unexpected values
  val expectedStrings = Set(null, "") ++ expectedValues.map(_.toString)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    if (!expectedStrings.contains(input.getString(0))) {
      buffer(0) = buffer.getLong(0) + 1
    }
  }
}