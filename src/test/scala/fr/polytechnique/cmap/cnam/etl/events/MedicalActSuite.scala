// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MedicalActSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  object MockMedicalAct extends MedicalAct {
    val category: EventCategory[MedicalAct] = "mock_act"
  }

  "apply" should "allow creation of a DiagnosisBuilder event" in {

    // Given
    val expected = Event[MedicalAct](patientID, MockMedicalAct.category, "hosp", "C67", 0.0, timestamp, None)

    // When
    val result = MockMedicalAct(patientID, "hosp", "C67", timestamp)

    // Then
    assert(result == expected)
  }
}
