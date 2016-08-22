package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer

import scala.reflect.ClassTag

/**
  * Created by sathiya on 26/07/16.
  */

/**
  * Same as the previous method, but instead of counting number of (column) values that are not in the list,
  * this UDAF counts number of values that are <i>in</i> the list.
  *
  * @param expectedValues
  * @tparam T type of the given column.
  */
class CustomAggExpectedValues[T: ClassTag](expectedValues: Set[T]) extends CustomAggUnexpectedValues[T](expectedValues) {

  val castedExpectedValues_expected = expectedValues.map(_.toString)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    if (castedExpectedValues_expected.contains(input.getString(0))) {
      buffer(0) = buffer.getLong(0) + 1
    }
  }
}