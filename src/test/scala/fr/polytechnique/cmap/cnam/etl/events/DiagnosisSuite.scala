// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.util.functions._

class DiagnosisSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  object MockDiagnosis extends Diagnosis {
    val category: EventCategory[Diagnosis] = "mock_diagnosis"
  }

  "apply" should "allow creation of a DiagnosisBuilder event" in {

    // Given
    val expected = Event[Diagnosis](patientID, MockDiagnosis.category, "NA", "C67", 0.0, timestamp, None)

    // When
    val result = MockDiagnosis(patientID, "C67", timestamp)

    // Then
    assert(result == expected)
  }

  "fromRow" should "allow creation of a DiagnosisBuilder event from a row object" in {

    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
        StructField("gId", StringType) ::
        StructField("cod", StringType) ::
        StructField("wei", StringType) ::
        StructField("dat", TimestampType) :: Nil
    )
    val values = Array[Any]("Patient01", "1_1_2010", "C67", 1.0, makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = MockDiagnosis("Patient01", "1_1_2010", "C67", 1.0, makeTS(2010, 1, 1))

    // When
    val result = MockDiagnosis.fromRow(r, "pID", "gId", "cod", "wei", "dat")

    // Then
    assert(result == expected)
  }

  it should "support creation without groupId" in {

    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
        StructField("cod", StringType) ::
        StructField("dat", TimestampType) :: Nil
    )
    val values = Array[Any]("Patient01", "C67", makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = MockDiagnosis("Patient01", "C67", makeTS(2010, 1, 1))

    // When
    val result = MockDiagnosis.fromRow(r, "pID", "cod", "dat")

    // Then
    assert(result == expected)
  }
}
