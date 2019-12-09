package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.util.functions.makeTS


class FollowUpSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val endReason:String = "any_reason"
  val start: Timestamp = mock(classOf[Timestamp])
  val end: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of an FollowUp event" in {
    // Given
    val expected = Event[FollowUp.type](patientID, FollowUp.category, "NA", endReason, 0.0, start, Some(end))
    // When
    val result = FollowUp(patientID,endReason,start,end)
    // Then
    assert(result == expected)
  }

  "fromRow" should "allow creation of a FollowUp event from a row object" in {
    // Given
    val schema = StructType(
        StructField("pID", StringType) ::
        StructField("endR", StringType) ::
        StructField("start", TimestampType) ::
        StructField("end", TimestampType) :: Nil
    )
    val values = Array[Any]("Patient01", "any_reason",  makeTS(2010, 1, 1), makeTS(2010, 2, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = FollowUp("Patient01", "any_reason",  makeTS(2010, 1, 1), makeTS(2010, 2, 1))

    // When
    val result = FollowUp.fromRow(r, "pID", "endR", "start", "end")

    // Then
    assert(result == expected)
  }
}
