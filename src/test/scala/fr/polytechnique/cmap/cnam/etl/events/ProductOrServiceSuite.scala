// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec

class ProductOrServiceSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of a Medical Act event" in {

    // Given
    val expected = Event[ProductOrService](patientID, ProductOrService.category, "hosp", "C67", 0.0, timestamp, None)

    // When
    val result = ProductOrService(patientID, "hosp", "C67", 0.0, timestamp)

    // Then
    assert(result == expected)
  }
}
