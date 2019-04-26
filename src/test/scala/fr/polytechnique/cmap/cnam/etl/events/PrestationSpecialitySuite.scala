package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec

class PrestationSpecialitySuite extends FlatSpec {

  object MockPrestationSpeciality extends PrestationSpeciality{
    val category: EventCategory[PrestationSpeciality] = "mock_prestationSpeciality"
  }

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of a PrestationSpecialityBuilder event" in {

    // Given
    val expected = Event[PrestationSpeciality](patientID, MockPrestationSpeciality.category, "A10000001", "42", 0.0, timestamp, None)

    // When
    val result = MockPrestationSpeciality(patientID, "A10000001","42", timestamp)

    // Then
    assert(result == expected)
  }

  "fromRow" should "allow creation of a PrestationSpecialityBuilder event from a row object" in {

    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
      StructField("gId", StringType) ::
      StructField("cod", StringType) ::
      StructField("dat", TimestampType) :: Nil)
    val values = Array[Any]("Patient01", "A10000001", "42", makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = MockPrestationSpeciality("Patient01", "A10000001", "42", makeTS(2010, 1, 1))

    // When
    val result = MockPrestationSpeciality.fromRow(r, "pID", "gId", "cod", "dat")

    // Then
    assert(result == expected)
  }
}
