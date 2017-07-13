package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.ColumnNames

class EventRowExtractorSuite extends SharedContext {

  object MockRowExtractor extends EventRowExtractor with ColumnNames {
    def extractPatientId(r: Row) = ""
    def extractStart(r: Row) = new Timestamp(0)
  }

  "extractGroupId" should "return the group ID for imb (always 'imb')" in {

    // Given
    val expected = "NA"

    // When
    val result = MockRowExtractor.extractGroupId(Row())

    // Then
    assert(result == expected)
  }


  "weight" should "return the weight value" in {

    // Given
    val expected = 0.0

    // When
    val result = MockRowExtractor.extractWeight(Row())

    // Then
    assert(result == expected)
  }


  "end" should "compute the end date of the event" in {

    // Given
    val expected: Option[Timestamp] = None

    // When
    val result = MockRowExtractor.extractEnd(Row())

    // Then
    assert(result == expected)
  }
}
