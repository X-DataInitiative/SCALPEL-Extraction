package fr.polytechnique.cmap.cnam.util

import java.sql.Timestamp
import org.scalatest.FlatSpecLike
import fr.polytechnique.cmap.cnam.util.functions._

class FunctionsSuite extends FlatSpecLike {

  "makeTS" should "build the correct timestamp" in {

    // Given
    val expected = List(
      "2010-01-24 14:23:15",
      "1901-09-21 01:12:59",
      "2037-12-03 00:00:00"
    ).map(Timestamp.valueOf)

    // When
    val result = List(
      makeTS(2010, 1, 24, 14, 23, 15),
      makeTS(1901, 9, 21, 1, 12, 59),
      makeTS(2037, 12, 3)
    )

    // Then
    assert(result == expected)
  }

  it should "throw an exception when any parameter is out of bounds" in {

    // Given

    // Then
    intercept[java.lang.IllegalArgumentException] {
      makeTS(19355, 1, 12)
    }
    intercept[java.lang.IllegalArgumentException] {
      makeTS(1935, 21, 7)
    }
    intercept[java.lang.IllegalArgumentException] {
      makeTS(1935, 12, 32)
    }
    intercept[java.lang.IllegalArgumentException] {
      makeTS(1935, 12, 23, 59, 59, 59)
    }
    intercept[java.lang.IllegalArgumentException] {
      makeTS(1935, 12, 23, 23, 99, 99)
    }
  }


  "makeTS" should "build the correct timestamp when input argument is List[Int]" in {

    // Given
    val expected = List(
      "2010-01-24 14:23:15",
      "1901-09-21 01:12:59",
      "2037-12-03 00:00:00"
    ).map(Timestamp.valueOf)

    // When
    val result = List(
      makeTS(List(2010, 1, 24, 14, 23, 15)),
      makeTS(List(1901, 9, 21, 1, 12, 59)),
      makeTS(List(2037, 12, 3))
    )

    // Then
    assert(result == expected)
  }

  it should "throw an exception when the input list dosen't match any case" in {

    // Given

    // Then
    intercept[java.lang.IllegalArgumentException] {
      makeTS(List())
    }
    intercept[java.lang.IllegalArgumentException] {
      makeTS(List(1935))
    }
    intercept[java.lang.IllegalArgumentException] {
      makeTS(List(1935, 12))
    }
    intercept[java.lang.IllegalArgumentException] {
      makeTS(List(1935, 12, 23, 59, 59, 59, 59))
    }
  }

}
