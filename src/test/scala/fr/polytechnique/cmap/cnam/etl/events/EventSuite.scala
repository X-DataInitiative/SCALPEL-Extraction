// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec

class EventSuite extends FlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  trait SomeEvent extends AnyEvent

  trait AnotherEvent extends AnyEvent

  object SomeEvent extends SomeEvent {
    val category: EventCategory[SomeEvent] = "some_category"
  }

  "apply" should "allow creation of new Events" in {

    // Given
    val expected: Event[SomeEvent] = Event(patientID, "some_category", "some_group", "some_value", 0.0, timestamp, None)

    // When
    val result: Event[SomeEvent] = Event(
      patientID,
      SomeEvent.category,
      "some_group",
      "some_value",
      0.0,
      timestamp,
      None
    )

    // Then
    assert(result == expected)
  }

  it should "fail if types don't match" in {
    assertTypeError(
      "val e: Event[AnotherEvent] = Event(patient, SomeEvent, \"some_id\", 0.0, timestamp, None)"
    )
  }

  "checkValue" should "check if the event's value matches a given value for the given category" in {
    // Given
    val someEvent = Event[SomeEvent](patientID, SomeEvent.category, "some_group", "some_value", 0.0, timestamp, None)

    // When|Then
    assert(someEvent.checkValue(SomeEvent.category, "some_value"))
    assert(!someEvent.checkValue(SomeEvent.category, "wrong_value"))
    assert(!someEvent.checkValue("wrong_category", "some_value"))
  }

  "checkValue" should "check if the event's value is found in a list for a given category" in {
    // Given
    val someEvent = Event[SomeEvent](patientID, SomeEvent.category, "some_group", "some_value", 0.0, timestamp, None)
    val values: List[String] = List("some_value", "another_value")

    // When|Then
    assert(someEvent.checkValue(SomeEvent.category, values))
    assert(!someEvent.checkValue(SomeEvent.category, Nil))
    assert(!someEvent.checkValue("wrong_category", values))
  }

  "checkValueStart" should "check if the event's value start with a given value for the given category" in {
    // Given
    val someEvent = Event[SomeEvent](patientID, SomeEvent.category, "some_group", "some_value", 0.0, timestamp, None)

    // When|Then
    assert(someEvent.checkValueStart(SomeEvent.category, "some_val"))
    assert(!someEvent.checkValueStart(SomeEvent.category, "wrong_value"))
    assert(!someEvent.checkValueStart("wrong_category", "some_value"))
  }

  "checkValueStart" should "check if the event's value starts with a value in a list for a given category" in {
    // Given
    val someEvent = Event[SomeEvent](patientID, SomeEvent.category, "some_group", "some_value", 0.0, timestamp, None)
    val values: List[String] = List("some_value", "another_value")

    // When|Then
    assert(someEvent.checkValueStart(SomeEvent.category, values))
    assert(!someEvent.checkValueStart(SomeEvent.category, Nil))
    assert(!someEvent.checkValueStart("wrong_category", values))
  }
}
