package fr.polytechnique.cmap.cnam.filtering

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.implicits._
import org.apache.spark.sql.Dataset
/**
  * Created by burq on 10/08/16.
  */
class DataWriterSuite extends SharedContext{

  "writeFlatEvent" should "write a file readable as a Dataset[FlatEvent]" in {
    // Given
    val events: Dataset[Event] =   ???
    val patients: Dataset[Patient] =   ???
    val path = "anyPath/flatEvents.parquet"


    // When
    events.writeFlatEvent(patients, path)

    val result = sqlContext.read.load(path)

    // Then
    assert(true)  // todo how to test this?

  }
}
