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
}
