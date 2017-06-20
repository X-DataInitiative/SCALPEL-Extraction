package fr.polytechnique.cmap.cnam.util.datetime

import org.scalatest.FlatSpec

class RichDateSuite extends FlatSpec {

  "Plus operator" should "allow adding a period to a java.sql.Date" in {
    // Given
    val period = Period(days = 3)
    val inputDate = new RichDate(java.sql.Date.valueOf("2010-01-01"))
    val expected = java.sql.Date.valueOf("2010-01-04")

    // When
    val result = inputDate + period

    // Then
    assert(result == expected)
  }

  it should "allow adding a period to a java.sql.Timestamp" in {
    // Given
    val period = Period(days = 3, hours = 25)
    val inputTimestamp = new RichDate(java.sql.Timestamp.valueOf("2010-01-01 15:00:00"))
    val expected = java.sql.Timestamp.valueOf("2010-01-05 16:00:00")

    // When
    val result = inputTimestamp + period

    // Then
    assert(result == expected)
  }

  "Minus operator" should "allow subtracting a period from a java.sql.Date" in {
    // Given
    val period = Period(days = 3)
    val inputDate = new RichDate(java.sql.Date.valueOf("2010-01-01"))
    val expected = java.sql.Date.valueOf("2009-12-29")

    // When
    val result = inputDate - period

    // Then
    assert(result == expected)
  }

  it should "allow subtracting a period from a java.sql.Timestamp" in {
    // Given
    val period = Period(days = 3, hours = 25)
    val inputTimestamp = new RichDate(java.sql.Timestamp.valueOf("2010-01-01 15:00:00"))
    val expected = java.sql.Timestamp.valueOf("2009-12-28 14:00:00")

    // When
    val result = inputTimestamp - period

    // Then
    assert(result == expected)
  }

  "between" should "return true if this date/timestamp is between two given dates/timestamps" in {
    // Given
    val inputDate = new RichDate(java.sql.Date.valueOf("2010-01-01"))
    val lowerBound = java.sql.Date.valueOf("2010-01-01")
    val upperBound = java.sql.Date.valueOf("2010-01-02")

    // When
    val result = inputDate.between(lowerBound, upperBound)

    // Then
    assert(result)
  }

  it should "not include bounds when false is passed as third parameter" in {
    // Given
    val inputDate = new RichDate(java.sql.Date.valueOf("2010-01-01"))
    val lowerBound = java.sql.Date.valueOf("2010-01-01")
    val upperBound = java.sql.Date.valueOf("2010-01-02")

    // When
    val result = inputDate.between(lowerBound, upperBound, includeBounds = false)

    // Then
    assert(!result)
  }
}
