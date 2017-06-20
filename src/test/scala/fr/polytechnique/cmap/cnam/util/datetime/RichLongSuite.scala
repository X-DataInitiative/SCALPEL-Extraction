package fr.polytechnique.cmap.cnam.util.datetime

import org.scalatest.FlatSpec

class RichLongSuite extends FlatSpec {

  "millisecond/milliseconds" should "return the corresponding Period instance" in {
    // Given
    val one = new RichLong(1L)
    val five = new RichLong(5L)

    // Then
    assert(one.ms == Period(milliseconds = 1))
    assert(one.millisecond == Period(milliseconds = 1))
    assert(five.milliseconds == Period(milliseconds = 5))
  }
}
