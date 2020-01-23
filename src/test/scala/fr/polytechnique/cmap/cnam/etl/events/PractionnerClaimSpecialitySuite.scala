// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.util.functions._

class PractitionerClaimSpecialitySuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  object MockPractionnerClaimSpeciality$ extends PractitionerClaimSpeciality {
    val category: EventCategory[PractitionerClaimSpeciality] = "mock_prestationSpeciality"
  }

  "apply" should "allow creation of a PrestationSpecialityBuilder event" in {

    // Given
    val expected = Event[PractitionerClaimSpeciality](
      patientID,
      MockPractionnerClaimSpeciality$.category,
      "A10000001",
      "42",
      0.0,
      timestamp,
      None
    )

    // When
    val result = MockPractionnerClaimSpeciality$(patientID, "A10000001", "42", timestamp)

    // Then
    assert(result == expected)
  }
}
