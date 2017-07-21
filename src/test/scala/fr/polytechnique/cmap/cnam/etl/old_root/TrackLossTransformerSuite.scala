package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources

/**
  * Created by burq on 07/09/16.
  */
class TrackLossTransformerSuite extends SharedContext {

  "withInterval" should "add the number of month before the next prescription" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_01", Timestamp.valueOf("2006-01-05 00:00:00")),
      ("Patient_01", Timestamp.valueOf("2006-11-05 00:00:00")),
      ("Patient_01", Timestamp.valueOf("2007-02-05 00:00:00"))
    ).toDF("patientID", "eventDate")

    val expected = Seq(
      ("Patient_01", Timestamp.valueOf("2006-01-05 00:00:00"), 10),
      ("Patient_01", Timestamp.valueOf("2006-11-05 00:00:00"), 3),
      ("Patient_01", Timestamp.valueOf("2007-02-05 00:00:00"), 34)
    ).toDF("patientID", "eventDate", "interval")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.TrackLossTransformer._
    val result = input.withInterval

    // Then
    assertDFs(result, expected)
  }

  "filterTrackLosses" should "remove any line with small interval" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_01", Timestamp.valueOf("2006-01-05 00:00:00"), 10),
      ("Patient_01", Timestamp.valueOf("2006-11-05 00:00:00"), 3),
      ("Patient_01", Timestamp.valueOf("2007-02-05 00:00:00"), 34)
    ).toDF("patientID", "eventDate", "interval")

    val expected =  Seq(
      ("Patient_01", Timestamp.valueOf("2006-01-05 00:00:00"), 10),
      ("Patient_01", Timestamp.valueOf("2007-02-05 00:00:00"), 34)
    ).toDF("patientID", "eventDate", "interval")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.TrackLossTransformer._
    val result = input.filterTrackLosses

    // Then
    assertDFs(result, expected)

  }

  "withTrackLossDate" should "add the date of the trackloss" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_01", Timestamp.valueOf("2006-01-05 00:00:00"), 10),
      ("Patient_01", Timestamp.valueOf("2007-02-05 00:00:00"), 34)
    ).toDF("patientID", "eventDate", "interval")

    val expected =  Seq(
      ("Patient_01", Timestamp.valueOf("2006-01-05 00:00:00"), 10,  Timestamp.valueOf("2006-03-05 00:00:00")),
      ("Patient_01", Timestamp.valueOf("2007-02-05 00:00:00"), 34,  Timestamp.valueOf("2007-04-05 00:00:00"))
    ).toDF("patientID", "eventDate", "interval", "trackloss")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.TrackLossTransformer._
    val result = input.withTrackLossDate

    // Then
    assertDFs(result, expected)

  }

  "transform" should "return correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = sqlContext.read.load("src/test/resources/test-input/DCIR.parquet")
    val sources = new Sources(dcir = Some(dcir))
    val expected =  Seq(
      Event("Patient_01", "trackloss", "eventId", 1.0, Timestamp.valueOf("2006-03-30 00:00:00"), None),
      Event("Patient_02", "trackloss", "eventId", 1.0, Timestamp.valueOf("2006-03-30 00:00:00"), None)
    ).toDF

    // When
    val result = TrackLossTransformer.transform(sources)

    // Then
    assertDFs(result.toDF(), expected)
 }
}
