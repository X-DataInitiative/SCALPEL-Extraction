package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.util.functions._

class PractitionerClaimSpecialitySuite extends FlatSpec {

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

  "fromRow" should "allow creation of a PrestationSpecialityBuilder event from a row object" in {

    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
        StructField("gId", StringType) ::
        StructField("cod", StringType) ::
        StructField("dat", TimestampType) :: Nil
    )
    val values = Array[Any]("Patient01", "A10000001", "42", makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = MockPractionnerClaimSpeciality$("Patient01", "A10000001", "42", makeTS(2010, 1, 1))

    // When
    val result = MockPractionnerClaimSpeciality$.fromRow(r, "pID", "gId", "cod", "dat")

    // Then
    assert(result == expected)
  }
}
