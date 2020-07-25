// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.util.functions._

class DiagnosisSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  object MockDiagnosis extends Diagnosis {
    val category: EventCategory[Diagnosis] = "mock_diagnosis"
  }

  "apply" should "allow creation of a DiagnosisBuilder event" in {

    // Given
    val expected = Event[Diagnosis](patientID, MockDiagnosis.category, "NA", "C67", 0.0, timestamp, None)

    // When
    val result = MockDiagnosis(patientID, "C67", timestamp)

    // Then
    assert(result == expected)
  }
}
