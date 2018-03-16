package fr.polytechnique.cmap.cnam.etl.extractors.tracklosses

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.Trackloss
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.DataFrame

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
    assertDFs(result, expected)
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
    assertDFs(result, expected)
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
    assertDFs(result, expected)
  }

  "extract" should "return correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = TracklossesConfig(makeTS(2006, 12, 31))
    val dcir: DataFrame = sqlContext.read.load("src/test/resources/test-input/DCIR.parquet")
    val sources = new Sources(dcir = Some(dcir))
    val expected: DataFrame = Seq(
      Trackloss("Patient_01", makeTS(2006, 3, 30)),
      Trackloss("Patient_02", makeTS(2006, 3, 30))
    ).toDF

    // When
    val result = new Tracklosses(config).extract(sources)

    // Then
    assertDFs(result.toDF(), expected)
 }
}
