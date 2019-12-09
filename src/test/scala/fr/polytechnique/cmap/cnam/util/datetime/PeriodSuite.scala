// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.datetime

import org.scalatest.flatspec.AnyFlatSpec

class PeriodSuite extends AnyFlatSpec {

  "A Period" should "be created directly by its fields" in {
    // Given|When
    val input = Period(7, 6, 5, 4, 3, 2, 1)
    // Then
    assert(input == new Period(90, 446582001L))
    assert(input == Period(months = 90, milliseconds = 446582001L))
    assert(input == Period.apply(7, 6, 5, 4, 3, 2, 1))
    assert(Period() == Period(0, 0, 0, 0, 0, 0, 0))
    assert(input.years == 7)
    assert(input.months == 6)
    assert(input.days == 5)
    assert(input.hours == 4)
    assert(input.minutes == 3)
    assert(input.seconds == 2)
    assert(input.milliseconds == 1)
  }

  "Plus operator" should "allow adding two period instances" in {
    // Given
    val input = Period(months = 1)
    val expected = Period(months = 5, hours = 3)
    // When
    val result = input + Period(months = 4, hours = 3)
    // Then
    assert(result == expected)
  }

  "The negation symbol" should "invert the signal for all fields" in {
    // Given
    val input = Period(1, 1, -1, -1, 1, 1, 0)
    val expected = Period(-1, -1, 1, 1, -1, -1, 0)
    // When
    val result = -input
    // Then
    assert(result == expected)
  }

  "toMap" should "convert the fields into an ordered map" in {
    // Given
    val period = Period(1, 2, 3, 4, 5, 6, 7)
    val expected = scala.collection.immutable.ListMap[String, Int](
      "years" -> 1,
      "months" -> 2,
      "days" -> 3,
      "hours" -> 4,
      "minutes" -> 5,
      "seconds" -> 6,
      "milliseconds" -> 7
    )
    // When
    val result = period.toMap
    // Then
    assert(result == expected)
  }

  "canEqual" should "correctly identify if the equality is possible" in {
    assert(Period().canEqual(Period(1, 2)))
    assert(!Period(1, 2).canEqual(List(1, 2)))
  }

  "equals" should "correctly identify equal and different periods" in {
    // Given
    val input = Period(1, 1, 1, 1, 1, 1, 2)

    // When|Then
    assert(input != List(1L, 1L, 1L, 1L, 1L, 1L, 2L))
    assert(Period() == Period())
    assert(input != Period())
    assert(Period() != input)
    assert(input == Period(1, 1, 1, 1, 1, 1, 2))
    assert(input != Period(1, 1, 1, 1, 1, 1, 1))
    assert(input != Period(2, 1, 1, 1, 1, 1, 1))
  }

  "toString" should "convert the period object into a readable string" in {
    assert(Period().toString == "Empty Period")
    assert(Period(months = 3).toString == "3 months")
    assert(Period(months = 3, years = 1).toString == "1 year, 3 months")
  }
}
