package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import org.apache.spark.sql.Dataset

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.implicits._
/**
  * Created by burq on 10/08/16.
  */
class DataWriterSuite extends SharedContext{

  "writeFlatEvent" should "write a file readable as a Dataset[FlatEvent]" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val events: Dataset[Event] = Seq(
      Event("patrick", "disease", "C67", 1.00, Timestamp.valueOf("1942-01-30 00:00:00"), None),
      Event("bob", "theSponge", "412", 42.00, Timestamp.valueOf("2001-01-01 00:00:00"), None)
    ).toDS()

    val patients: Dataset[Patient] = Seq(
      Patient("patrick", 3, Timestamp.valueOf("1942-01-30 00:00:00"), None),
      Patient("bob", 12, Timestamp.valueOf("1901-01-01 00:00:00"), Some(Timestamp.valueOf("2001-01-01 00:00:00")))
    ).toDS()

    val path = "anyPath/flatEvents.parquet"


    // When
    events.writeFlatEvent(patients, path)

    val result = sqlContext.read.load(path)

    // Then
    assert(true)  // todo how to test this?

  }
}
