package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MedicalActSuite extends FlatSpec {

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

  "fromRow" should "allow creation of a DiagnosisBuilder event from a row object" in {

    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
        StructField("gId", StringType) ::
        StructField("cod", StringType) ::
        StructField("dat", TimestampType) :: Nil
    )
    val values = Array[Any]("Patient01", "1_1_2010", "C67", makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = MockMedicalAct("Patient01", "1_1_2010", "C67", makeTS(2010, 1, 1))

    // When
    val result = MockMedicalAct.fromRow(r, "pID", "gId", "cod", "dat")

    // Then
    assert(result == expected)
  }
}
