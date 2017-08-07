package fr.polytechnique.cmap.cnam.util

import java.sql.Timestamp
import org.scalatest.FlatSpecLike

class FunctionsSuite extends FlatSpecLike {

  "parseTimestamp" should "convert a string to a Timestamp if it's not null and non-empty" in {

    // Given
    val input1 = "1991-03-15 18:21:00.000"
    val input2 = "15/03/91 18:21"
    val pattern2 = "dd/MM/yy HH:mm"
    val expected = Some(Timestamp.valueOf("1991-03-15 18:21:00"))

    // When
    val result1 = functions.parseTimestamp(input1)
    val result2 = functions.parseTimestamp(input2, pattern2)

    // Then
    assert(result1 == expected)
    assert(result2 == expected)
  }

  it should "return None if the input string is empty or null" in {

    // Given
    val input1: String = """
                         """
    val input2: String = null
    val expected: Option[Timestamp] = None

    // When
    val result1 = functions.parseTimestamp(input1)
    val result2 = functions.parseTimestamp(input2)

    // Then
    assert(result1 == expected)
    assert(result2 == expected)
  }

  it should "throw an exception if parsing is not possible" in {

    // Given
    val pattern = "dd/MM/yyyy HH:mm"
    val input = "01/15/2010 23:59"

    // WhenThen
    intercept[functions.DateParseException] {
      functions.parseTimestamp(input, pattern)
    }
  }

  "parseDate" should "convert a string to a java.sql.Date if it's not null and non-empty" in {

    // Given
    val input1 = "1991-03-15"
    val input2 = "15/03/91"
    val pattern2 = "dd/MM/yy"
    val expected = Some(java.sql.Date.valueOf("1991-03-15"))

    // When
    val result1 = functions.parseDate(input1)
    val result2 = functions.parseDate(input2, pattern2)

    // Then
    assert(result1 == expected)
    assert(result2 == expected)
  }

  it should "return None if the input string is empty or null" in {

    // Given
    val input1: String = """
                         """
    val input2: String = null
    val expected: Option[java.sql.Date] = None

    // When
    val result1 = functions.parseDate(input1)
    val result2 = functions.parseDate(input2)

    // Then
    assert(result1 == expected)
    assert(result2 == expected)
  }


  it should "throw an exception if parsing is not possible" in {
    // Given
    val pattern = "dd/MM/yyyy"
    val input = "01/15/2010"

    // WhenThen
    intercept[functions.DateParseException] {
      functions.parseDate(input, pattern)
    }
  }

  "functions.makeTS" should "build the correct timestamp" in {

    // Given
    val expected = List(
      "2010-01-24 14:23:15",
      "1901-09-21 01:12:59",
      "2037-12-03 00:00:00"
    ).map(Timestamp.valueOf)

    // When
    val result = List(
      functions.makeTS(2010, 1, 24, 14, 23, 15),
      functions.makeTS(1901, 9, 21, 1, 12, 59),
      functions.makeTS(2037, 12, 3)
    )

    // Then
    assert(result == expected)
  }

  it should "throw an exception when any parameter is out of bounds" in {

    // Given

    // Then
    intercept[java.lang.IllegalArgumentException] {
      functions.makeTS(19355, 1, 12)
    }
    intercept[java.lang.IllegalArgumentException] {
      functions.makeTS(1935, 21, 7)
    }
    intercept[java.lang.IllegalArgumentException] {
      functions.makeTS(1935, 12, 32)
    }
    intercept[java.lang.IllegalArgumentException] {
      functions.makeTS(1935, 12, 23, 59, 59, 59)
    }
    intercept[java.lang.IllegalArgumentException] {
      functions.makeTS(1935, 12, 23, 23, 99, 99)
    }
  }


  "functions.makeTS" should "build the correct timestamp when input argument is List[Int]" in {

    // Given
    val expected = List(
      "2010-01-24 14:23:15",
      "1901-09-21 01:12:59",
      "2037-12-03 00:00:00"
    ).map(Timestamp.valueOf)

    // When
    val result = List(
      functions.makeTS(List(2010, 1, 24, 14, 23, 15)),
      functions.makeTS(List(1901, 9, 21, 1, 12, 59)),
      functions.makeTS(List(2037, 12, 3))
    )

    // Then
    assert(result == expected)
  }

  it should "throw an exception when the input list dosen't match any case" in {

    // Given

    // Then
    intercept[java.lang.IllegalArgumentException] {
      functions.makeTS(List())
    }
    intercept[java.lang.IllegalArgumentException] {
      functions.makeTS(List(1935))
    }
    intercept[java.lang.IllegalArgumentException] {
      functions.makeTS(List(1935, 12))
    }
    intercept[java.lang.IllegalArgumentException] {
      functions.makeTS(List(1935, 12, 23, 59, 59, 59, 59))
    }
  }

}
