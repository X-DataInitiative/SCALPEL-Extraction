package fr.polytechnique.cmap.cnam.etl.events.acts

import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.etl.events.{Event, EventCategory}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MedicalActSuite extends FlatSpec {

  object MockMedicalAct extends MedicalAct {
    val category: EventCategory[MedicalAct] = "mock_act"
  }

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of a DiagnosisBuilder event" in {

    // Given
    val expected = Event[MedicalAct](patientID, MockMedicalAct.category, "hosp", "C67", 0.0, timestamp, None)

    // When
    val result = MockMedicalAct(patientID, "hosp", "C67", timestamp)

    // Then
    assert(result == expected)
  }

  "fromRow" should "allow creation of a DiagnosisBuilder event from a row object" in {

    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
      StructField("gId", StringType) ::
      StructField("cod", StringType) ::
      StructField("dat", TimestampType) :: Nil)
    val values = Array[Any]("Patient01", "1_1_2010", "C67", makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = MockMedicalAct("Patient01", "1_1_2010", "C67", makeTS(2010, 1, 1))

    // When
    val result = MockMedicalAct.fromRow(r, "pID", "gId", "cod", "dat")

    // Then
    assert(result == expected)
  }
}
