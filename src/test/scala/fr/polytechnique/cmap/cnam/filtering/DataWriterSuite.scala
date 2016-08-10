package fr.polytechnique.cmap.cnam.filtering

import java.io.File
import java.sql.Timestamp

import org.apache.spark.sql.Dataset
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.implicits._
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
/**
  * Created by burq on 10/08/16.
  */
class DataWriterSuite extends SharedContext with BeforeAndAfter{

  override def beforeEach(): Unit ={
    val directory = new File("anyPath")
    FileUtils.deleteDirectory(directory)
    super.afterEach()
  }

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

    val expected: Dataset[FlatEvent] = Seq(
      FlatEvent("patrick", 3, Timestamp.valueOf("1942-01-30 00:00:00"), None,
        "disease", "C67", 1.00, Timestamp.valueOf("1942-01-30 00:00:00"), None),
      FlatEvent("bob", 12, Timestamp.valueOf("1901-01-01 00:00:00"), Some(Timestamp.valueOf("2001-01-01 00:00:00")),
        "theSponge", "412", 42.00, Timestamp.valueOf("2001-01-01 00:00:00"), None)
    ).toDS()


    // When
    events.writeFlatEvent(patients, path)
    val result = sqlContext.read.load(path).as[FlatEvent]

    // Then
    assert(result.collect.toSeq.sortBy(_.patientID) == expected.collect.toSeq.sortBy(_.patientID))

  }
}
