package fr.polytechnique.cmap.cnam.etl.events.acts

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.events.{Event, EventCategory}
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec

class ActBuilderSuite extends FlatSpec {

  object MockAct extends MedicalActBuilder {
    val category: EventCategory[MedicalAct] = "mock_diagnosis"
  }

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of a ActBuilder event" in {

    // Given
    val expected = Event[MedicalAct](patientID, MockAct.category, "C67", 0.0, timestamp, None)

    // When
    val result = MockAct(patientID, "C67", timestamp)

    // Then
    assert(result == expected)
  }

  "fromRow" should "allow creation of a MedicalActBuilder event from a row object" in {

    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
      StructField("cod", StringType) ::
      StructField("dat", TimestampType) :: Nil)
    val values = Array[Any]("Patient01", "C67", makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = MockAct("Patient01", "C67", makeTS(2010, 1, 1))

    // When
    val result = MockAct.fromRow(r, "pID", "cod", "dat")

    // Then
    assert(result == expected)
  }
}
