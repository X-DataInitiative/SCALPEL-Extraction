// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.SharedContext

class EventRowExtractorSuite extends SharedContext {

  object MockRowExtractor extends EventRowExtractor with ColumnNames {
    def extractPatientId(r: Row) = ""

    def extractStart(r: Row) = new Timestamp(0)
  }

  "extractGroupId" should "return NA" in {

    // Given
    val expected = "NA"

    // When
    val result = MockRowExtractor.extractGroupId(Row())

    // Then
    assert(result == expected)
  }

  "extractValue" should "return NA" in {

    // Given
    val expected = "NA"

    // When
    val result = MockRowExtractor.extractGroupId(Row())

    // Then
    assert(result == expected)
  }


  "weight" should "return 0.0" in {

    // Given
    val expected = 0.0

    // When
    val result = MockRowExtractor.extractWeight(Row())

    // Then
    assert(result == expected)
  }


  "end" should "return None" in {

    // Given
    val expected: Option[Timestamp] = None

    // When
    val result = MockRowExtractor.extractEnd(Row())

    // Then
    assert(result == expected)
  }
}
