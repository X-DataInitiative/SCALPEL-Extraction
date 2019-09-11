package fr.polytechnique.cmap.cnam.util

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.fs.{Path => HDFSPath}
import org.scalatest.FlatSpec

class PathSuite extends FlatSpec {

  "apply" should "correctly return a org.apache.hadoop.fs.Path instance" in {
    // Given
    val paths = Seq(
      Path("/left", "right"),
      Path("/left/", "right"),
      Path(new HDFSPath("/left"), new HDFSPath("right")),
      Path("/left", new HDFSPath("right")),
      Path(new HDFSPath("/left"), "right")
    )

    val expected: HDFSPath = new HDFSPath("/left", "right")

    // Then
    paths.foreach { p =>
      assert(p == expected)
    }
  }

  it should "also work for a single string" in {
    // Given
    val path = Path("/left")
    val expected = new HDFSPath("/left")

    // Then
    assert(path == expected)
  }

  it should "also work for a multiple strings" in {
    // Given
    val path1 = Path("/first", "second", "third")
    val path2 = Path(Path("/first"), "second", "third")
    val expected = new HDFSPath("/first/second/third")

    // Then
    assert(path1 == expected)
    assert(path2 == expected)
  }

  "withTimestampSuffix" should "add a timestamp at the end of the path" in {
    //Given
    val path = Path("/first", "second", "third/")
    val format = new SimpleDateFormat("_yyyy_MM_dd_HH_mm_ss")
    val now = new Date()
    val expected = s"/first/second/third${format.format(now)}"

    //When
    val result = path.withTimestampSuffix(now).toString

    //Then
    assert(result == expected)
  }

}
