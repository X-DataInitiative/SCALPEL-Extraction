// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import fr.polytechnique.cmap.cnam.{SharedContext, util}

class DrugSuite extends SharedContext {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of Drug event" in {

    // Given
    val expected = Event[Drug](
      patientID, Drug.category, "NA", "Drug1", 0.1, timestamp, None
    )

    // When
    val result = Drug(patientID, "Drug1", 0.1, "NA", timestamp)

    // Then
    assert(result == expected)
  }


}
