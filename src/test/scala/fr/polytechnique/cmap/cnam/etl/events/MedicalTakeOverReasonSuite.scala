package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec

class MedicalTakeOverReasonSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  object MockMedicalTakeOverReason extends  MedicalTakeOverReason {
    val category: EventCategory[ MedicalTakeOverReason] = "mock_take_over_reason"
  }

  "apply" should "allow creation of a MedicalTakeOverReason Builder event" in {

    // Given
    val expected = Event[MedicalTakeOverReason](patientID, MockMedicalTakeOverReason.category, "hosp", "11", 0.0, timestamp, None)

    // When
    val result = MockMedicalTakeOverReason(patientID, "hosp", "11", timestamp)

    // Then
    assert(result == expected)
  }
}
