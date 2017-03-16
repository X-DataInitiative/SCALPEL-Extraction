package fr.polytechnique.cmap.cnam.filtering.events

import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.utilities.functions._

class DiagnosisSuite extends FlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of a Diagnosis event" in {
    // Given
    val expected = Event[Diagnosis.type](patientID, Diagnosis.category, "C67", 0.0, timestamp, None)
    // When
    val result = Diagnosis(patientID, "C67", timestamp)
    // Then
    assert(result == expected)
  }

  "fromRow" should "allow creation of a Diagnosis event from a row object" in {
    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
      StructField("cod", StringType) ::
      StructField("dat", TimestampType) :: Nil)
    val values = Array[Any]("Patient01", "C67", makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = Diagnosis("Patient01", "C67", makeTS(2010, 1, 1))

    // When
    val result = Diagnosis.fromRow(r, "pID", "cod", "dat")

    // Then
    assert(result == expected)
  }
}
