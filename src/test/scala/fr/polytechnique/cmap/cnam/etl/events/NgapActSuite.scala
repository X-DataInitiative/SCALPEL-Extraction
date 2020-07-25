// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec

class NgapActSuite extends AnyFlatSpec {

  object MockNgapAct extends NgapAct

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of a NgapActBuilder event" in {

    // Given
    val expected = Event[NgapAct](patientID, MockNgapAct.category, "A10000001", "9.5", 0.0, timestamp, None)

    // When
    val result = MockNgapAct(patientID, "A10000001","9.5", timestamp)

    // Then
    assert(result == expected)
  }
}
