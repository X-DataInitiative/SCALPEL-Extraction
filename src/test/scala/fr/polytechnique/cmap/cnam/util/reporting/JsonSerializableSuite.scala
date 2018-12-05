package fr.polytechnique.cmap.cnam.util.reporting

import fr.polytechnique.cmap.cnam.util.Locales
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.scalatest.FlatSpec

class JsonSerializableSuite extends FlatSpec with Locales {

  case class TestItem(
    anInteger: Int,
    aDouble: Double,
    aString: String,
    anOption: Option[String],
    aDate: java.util.Date) extends JsonSerializable

  case class TestItemList(aList: List[TestItem]) extends JsonSerializable

  val input: TestItemList = {
    val item1 = TestItem(1, 1.0, "one", Some("one"), makeTS(2011, 1, 1))
    val item2 = TestItem(2, 2.0, "two", None, makeTS(2022, 2, 2))
    TestItemList(List(item1, item2))
  }

  "toJsonString" should "produce a nice pretty string" in {

    // Given
    val expected =
      """|{
         |  "a_list" : [ {
         |    "an_integer" : 1,
         |    "a_double" : 1.0,
         |    "a_string" : "one",
         |    "an_option" : "one",
         |    "a_date" : "2011-01-01T00:00:00Z"
         |  }, {
         |    "an_integer" : 2,
         |    "a_double" : 2.0,
         |    "a_string" : "two",
         |    "a_date" : "2022-02-02T00:00:00Z"
         |  } ]
         |}""".stripMargin

    // When
    val result = input.toJsonString(pretify = true)

    // Then
    assert(result == expected)
  }

  it should "produce a compact string if prettify=false" in {

    // Given
    val expected =
      """{""" +
        """"a_list":[""" +
          """{""" +
            """"an_integer":1,""" +
            """"a_double":1.0,""" +
            """"a_string":"one",""" +
            """"an_option":"one",""" +
            """"a_date":"2011-01-01T00:00:00Z"""" +
          """},""" +
          """{""" +
            """"an_integer":2,""" +
            """"a_double":2.0,""" +
            """"a_string":"two",""" +
            """"a_date":"2022-02-02T00:00:00Z"""" +
          """}""" +
        """]""" +
      """}"""

    // When
    val result = input.toJsonString(pretify = false)

    // Then
    assert(result == expected)
  }
}
