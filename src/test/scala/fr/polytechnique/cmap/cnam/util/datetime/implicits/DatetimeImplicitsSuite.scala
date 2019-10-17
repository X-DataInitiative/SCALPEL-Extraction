// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.datetime.implicits

import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.util.datetime._

class DatetimeImplicitsSuite extends FlatSpec {

  "this" should "implicitly convert Ints, Longs and Dates" in {
    // Given
    val intVal: Int = 15
    val longVal: Long = 150L
    val dateVal: java.sql.Date = java.sql.Date.valueOf("2010-01-01")

    // When
    val richIntVal: RichInt = intVal
    val richLongVal: RichLong = longVal
    val richDateVal: RichDate[java.sql.Date] = dateVal

    // Then
    assert(richIntVal.value == intVal)
    assert(richLongVal.value == longVal)
    assert(richDateVal.datetime == dateVal)
  }
}
