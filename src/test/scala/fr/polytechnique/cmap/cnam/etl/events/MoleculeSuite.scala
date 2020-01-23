// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MoleculeSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of a Molecule event" in {
    // Given
    val expected = Event[Molecule.type](
      patientID, Molecule.category, "NA", "pioglitazone", 100.0, timestamp, None
    )
    // When
    val result = Molecule(patientID, "pioglitazone", 100.0, timestamp)
    // Then
    assert(result == expected)
  }
}
