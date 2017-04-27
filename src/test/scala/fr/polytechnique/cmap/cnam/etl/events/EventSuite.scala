package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec

class EventSuite extends FlatSpec {

  trait SomeEvent extends AnyEvent
  object SomeEvent extends SomeEvent {
    val category: EventCategory[SomeEvent] = "some_category"
  }
  trait AnotherEvent extends AnyEvent

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of new Events" in {

    // Given
    val expected: Event[SomeEvent] = Event(patientID, "some_category", "some_group", "some_value", 0.0, timestamp, None)

    // When
    val result: Event[SomeEvent] = Event(patientID, SomeEvent.category, "some_group", "some_value", 0.0, timestamp, None)

    // Then
    assert(result == expected)
  }

  it should "fail if types don't match" in {
    assertTypeError(
      "val e: Event[AnotherEvent] = Event(patient, SomeEvent, \"some_id\", 0.0, timestamp, None)"
    )
  }
}
