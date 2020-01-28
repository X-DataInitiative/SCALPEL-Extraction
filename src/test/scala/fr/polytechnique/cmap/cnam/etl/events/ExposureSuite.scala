// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class ExposureSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val start: Timestamp = mock(classOf[Timestamp])
  val end: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of an Exposure event" in {
    // Given
    val expected = Event[Exposure.type](patientID, Exposure.category, "NA", "pioglitazone", 100.0, start, Some(end))
    // When
    val result = Exposure(patientID, "pioglitazone", 100.0, start, end)
    // Then
    assert(result == expected)
  }
}
