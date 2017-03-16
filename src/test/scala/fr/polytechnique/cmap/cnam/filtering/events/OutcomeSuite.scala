package fr.polytechnique.cmap.cnam.filtering.events

import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS

class OutcomeSuite extends FlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of an Outcome event" in {
    // Given
    val expected = Event[Outcome.type](patientID, Outcome.category, "bladder_cancer", 0.0, timestamp, None)
    // When
    val result = Outcome(patientID, "bladder_cancer", timestamp)
    // Then
    assert(result == expected)
  }

  "fromRow" should "allow creation of a Outcome event from a row object" in {
    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
        StructField("name", StringType) ::
        StructField("date", TimestampType) :: Nil)
    val values = Array[Any]("Patient01", "bladder_cancer", makeTS(2010, 1, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = Outcome("Patient01", "bladder_cancer", makeTS(2010, 1, 1))

    // When
    val result = Outcome.fromRow(r, "pID", "name", "date")

    // Then
    assert(result == expected)
  }
}
