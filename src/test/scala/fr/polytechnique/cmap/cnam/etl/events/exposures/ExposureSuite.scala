package fr.polytechnique.cmap.cnam.etl.events.exposures

import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class ExposureSuite extends FlatSpec {

  val patientID: String = "patientID"
  val start: Timestamp = mock(classOf[Timestamp])
  val end: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of an Exposure event" in {
    // Given
    val expected = Event[Exposure.type](patientID, Exposure.category, "pioglitazone", 100.0, start, Some(end))
    // When
    val result = Exposure(patientID, "pioglitazone", 100.0, start, end)
    // Then
    assert(result == expected)
  }

  "fromRow" should "allow creation of a Exposure event from a row object" in {
    // Given
    val schema = StructType(
      StructField("pID", StringType) ::
      StructField("mol", StringType) ::
      StructField("weight", DoubleType) ::
      StructField("start", TimestampType) ::
      StructField("end", TimestampType) :: Nil)
    val values = Array[Any]("Patient01", "pioglitazone", 100.0, makeTS(2010, 1, 1), makeTS(2010, 2, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = Exposure("Patient01", "pioglitazone", 100.0, makeTS(2010, 1, 1), makeTS(2010, 2, 1))

    // When
    val result = Exposure.fromRow(r, "pID", "mol", "weight", "start", "end")

    // Then
    assert(result == expected)
  }
}
