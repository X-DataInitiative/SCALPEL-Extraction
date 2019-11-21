package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec

class MedicalTakeOverReasonSuite extends FlatSpec {

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

  "fromRow" should "allow creation of a MedicalTakeOverReason Builder event from a row object" in {

    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
        StructField("gId", StringType) ::
        StructField("cod", StringType) ::
        StructField("wei", StringType) ::
        StructField("dat", TimestampType) :: Nil
    )
    val values = Array[Any]("Patient01", "1_1_2010", "11", 0.0, makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = MockMedicalTakeOverReason("Patient01", "1_1_2010", "11", 0.0, makeTS(2010, 1, 1))

    // When
    val result = MockMedicalTakeOverReason.fromRow(r, "pID", "gId", "cod", "wei", "dat")

    // Then
    assert(result == expected)
  }
}
