package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.etl.events.{Event, EventCategory}
import fr.polytechnique.cmap.cnam.util.functions._

class DiagnosisBuilderSuite extends FlatSpec {

  object MockDiagnosis extends DiagnosisBuilder {
    val category: EventCategory[Diagnosis] = "mock_diagnosis"
  }

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of a DiagnosisBuilder event" in {

    // Given
    val expected = Event[Diagnosis](patientID, MockDiagnosis.category, "", "C67", 0.0, timestamp, None)

    // When
    val result = MockDiagnosis(patientID, "C67", timestamp)

    // Then
    assert(result == expected)
  }

  "fromRow" should "allow creation of a DiagnosisBuilder event from a row object" in {

    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
      StructField("cod", StringType) ::
      StructField("dat", TimestampType) :: Nil)
    val values = Array[Any]("Patient01", "C67", makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = MockDiagnosis("Patient01", "C67", makeTS(2010, 1, 1))

    // When
    val result = MockDiagnosis.fromRow(r, "pID", "cod", "dat")

    // Then
    assert(result == expected)
  }
}
