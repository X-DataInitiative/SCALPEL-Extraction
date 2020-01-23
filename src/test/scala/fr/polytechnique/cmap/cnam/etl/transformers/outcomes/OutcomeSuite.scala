// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.outcomes

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.etl.events.{Event, Outcome}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class OutcomeSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of an Outcome event" in {
    // Given
    val expected = Event[Outcome.type](
      patientID, Outcome.category, "NA", "bladder_cancer", 0.0, timestamp, None
    )
    // When
    val result = Outcome(patientID, "bladder_cancer", timestamp)
    // Then
    assert(result == expected)
  }

  "apply" should "allow creation of an Outcome event with weight" in {
    // Given
    val expected = Event[Outcome.type](
      patientID, Outcome.category, "NA", "bladder_cancer", 2.0, timestamp, None
    )
    // When
    val result = Outcome(patientID, "bladder_cancer", 2.0, timestamp)
    // Then
    assert(result == expected)
  }
}
