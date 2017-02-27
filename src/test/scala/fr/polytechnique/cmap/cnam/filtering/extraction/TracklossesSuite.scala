package fr.polytechnique.cmap.cnam.filtering.extraction

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.Sources
import fr.polytechnique.cmap.cnam.filtering.events.Trackloss
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class TracklossesSuite extends SharedContext {

  "withInterval" should "add the number of month before the next prescription" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_01", makeTS(2006,  1,  5)),
      ("Patient_01", makeTS(2006, 11,  5)),
      ("Patient_01", makeTS(2007,  2,  5))
    ).toDF("patientID", "eventDate")

    val expected = Seq(
      ("Patient_01", makeTS(2006,  1,  5), 10),
      ("Patient_01", makeTS(2006, 11,  5), 3),
      ("Patient_01", makeTS(2007,  2,  5), 34)
    ).toDF("patientID", "eventDate", "interval")

    // When
    import Tracklosses.TracklossesDataFrame
    val result = input.withInterval(makeTS(2009, 12, 31))

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  "filterTrackLosses" should "remove any line with small interval" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_01", makeTS(2006,  1,  5), 10),
      ("Patient_01", makeTS(2006, 11,  5), 3),
      ("Patient_01", makeTS(2007,  2,  5), 34)
    ).toDF("patientID", "eventDate", "interval")

    val expected =  Seq(
      ("Patient_01", makeTS(2006,  1,  5), 10),
      ("Patient_01", makeTS(2007,  2,  5), 34)
    ).toDF("patientID", "eventDate", "interval")

    // When
    import Tracklosses.TracklossesDataFrame
    val result = input.filterTrackLosses(4)

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  "withTrackLossDate" should "add the date of the trackloss" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_01", makeTS(2006, 1, 5), 10),
      ("Patient_01", makeTS(2007, 2, 5), 34)
    ).toDF("patientID", "eventDate", "interval")

    val expected =  Seq(
      ("Patient_01", makeTS(2006, 1, 5), 10,  makeTS(2006, 3, 5)),
      ("Patient_01", makeTS(2007, 2, 5), 34,  makeTS(2007, 4, 5))
    ).toDF("patientID", "eventDate", "interval", "tracklossDate")

    // When
    import Tracklosses.TracklossesDataFrame
    val result = input.withTrackLossDate(2)

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  "extract" should "return correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExtractionConfig.init()
    val dcir: DataFrame = sqlContext.read.load("src/test/resources/expected/DCIR.parquet")
    val sources = new Sources(dcir = Some(dcir))
    val expected: DataFrame = Seq(
      Trackloss("Patient_01", makeTS(2006, 3, 30)),
      Trackloss("Patient_02", makeTS(2006, 3, 30))
    ).toDF

    // When
    val result = Tracklosses.extract(config, sources)

    // Then
    import RichDataFrames._
    assert(result.toDF() === expected)
  }
}
