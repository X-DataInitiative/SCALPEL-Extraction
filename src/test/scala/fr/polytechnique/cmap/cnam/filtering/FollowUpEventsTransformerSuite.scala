package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class FollowUpEventsTransformerSuite extends SharedContext {

  "withFollowUpStart" should "add a column with the start of the follow-up period" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", makeTS(2008, 1, 20), makeTS(2009, 12, 31)),
      ("Patient_A", makeTS(2008, 1, 20), makeTS(2009, 12, 31)),
      ("Patient_B", makeTS(2009, 1, 1), makeTS(2009, 12, 31)),
      ("Patient_B", makeTS(2009, 1, 1), makeTS(2009, 12, 31)),
      ("Patient_C", makeTS(2009, 10, 1), makeTS(2009, 12, 31)),
      ("Patient_C", makeTS(2009, 10, 1), makeTS(2009, 12, 31))
    ).toDF("patientID", "observationStart", "observationEnd")

    val expected = Seq(
      ("Patient_A", Some(makeTS(2008, 7, 20))),
      ("Patient_A", Some(makeTS(2008, 7, 20))),
      ("Patient_B", Some(makeTS(2009, 7, 1))),
      ("Patient_B", Some(makeTS(2009, 7, 1))),
      ("Patient_C", None),
      ("Patient_C", None)
    ).toDF("patientID", "followUpStart")

    // When
    import FollowUpEventsTransformer.FollowUpDataFrame
    val result = input.withFollowUpStart.select("patientID", "followUpStart")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }


  "withTrackLoss" should "add the date of the right trackloss event" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_D", "molecule", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "molecule", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "disease", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF("patientID", "category", "start", "followUpStart")

    val expected = Seq(
      ("Patient_D", "molecule", makeTS(2006, 8, 1)),
      ("Patient_D", "molecule", makeTS(2006, 8, 1)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1)),
      ("Patient_D", "disease", makeTS(2006, 8, 1))
    ).toDF("patientID", "category", "trackloss")

    // When
    import FollowUpEventsTransformer.FollowUpDataFrame
    val result = input.withTrackloss.select("patientID", "category", "trackloss")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  it should "get the first trackloss" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_D", "molecule", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "molecule", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "trackloss", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "trackloss", makeTS(2006, 12, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "disease", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF("patientID", "category", "start", "followUpStart")

    val expected = Seq(
      ("Patient_D", "molecule", makeTS(2006, 8, 1)),
      ("Patient_D", "molecule", makeTS(2006, 8, 1)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1)),
      ("Patient_D", "disease", makeTS(2006, 8, 1))
    ).toDF("patientID", "category", "trackloss")

    // When
    import FollowUpEventsTransformer.FollowUpDataFrame
    val result = input.withTrackloss.select("patientID", "category", "trackloss")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  it should "avoid useless trackloss" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_C", "molecule", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "molecule", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "trackloss", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "disease", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF("patientID", "category", "start", "followUpStart")

    val expected = Seq(
      ("Patient_C", "molecule"),
      ("Patient_C", "molecule"),
      ("Patient_C", "trackloss"),
      ("Patient_C", "disease")
    ).toDF("patientID", "category").withColumn("trackloss", lit(null).cast(TimestampType))

    // When
    import FollowUpEventsTransformer.FollowUpDataFrame
    val result = input.withTrackloss.select("patientID", "category", "trackloss")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withFollowUpEnd" should "add a column with the end of the follow-up period" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      // Cancer:
      ("Patient_A", Some(makeTS(2008, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 12, 1), None),
      ("Patient_A", Some(makeTS(2008, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 11, 1), None),
      ("Patient_A", Some(makeTS(2008, 1, 1)), "disease", "C67", makeTS(2007, 12, 1), None),
      // Death:
      ("Patient_B", Some(makeTS(2008, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 12, 1), None),
      ("Patient_B", Some(makeTS(2008, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 11, 1), None),
      // Track loss :
      ("Patient_C", Some(makeTS(2010, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 1, 1),
        Some(makeTS(2008, 2, 1))),
      ("Patient_C", Some(makeTS(2010, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 2, 1),
        Some(makeTS(2008, 2, 1))),
      // End of Observation:
      ("Patient_D", Some(makeTS(2016, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2016, 1, 1), None),
      ("Patient_D", Some(makeTS(2016, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2016, 2, 1), None),
      ("Patient_D", Some(makeTS(2016, 1, 1)), "disease", "C67", makeTS(2016, 3, 1), None)
    )
      .toDF("patientID", "deathDate", "category", "eventId", "start", "trackloss")
      .withColumn("observationEnd", lit(makeTS(2009, 12, 31, 23, 59, 59)))

    input.show

    val expected = Seq(
      ("Patient_A", makeTS(2007, 12, 1)),
      ("Patient_A", makeTS(2007, 12, 1)),
      ("Patient_A", makeTS(2007, 12, 1)),
      ("Patient_B", makeTS(2008, 1, 1)),
      ("Patient_B", makeTS(2008, 1, 1)),
      ("Patient_C", makeTS(2008, 2, 1)),
      ("Patient_C", makeTS(2008, 2, 1)),
      ("Patient_D", makeTS(2009, 12, 31, 23, 59, 59)),
      ("Patient_D", makeTS(2009, 12, 31, 23, 59, 59)),
      ("Patient_D", makeTS(2009, 12, 31, 23, 59, 59))
    ).toDF("patientID", "followUpEnd")

    // When
    import FollowUpEventsTransformer.FollowUpDataFrame
    val result = input.withFollowUpEnd.select("patientID", "followUpEnd")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withEndReason" should "add a column for the reason of follow-up end" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      (makeTS(2009, 12, 31), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2008, 12, 1),
        makeTS(2009, 12, 31)),
      (makeTS(2006, 12, 1), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2008, 12, 1),
        makeTS(2009, 12, 31)),
      (makeTS(2007, 12, 1), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2008, 12, 1),
        makeTS(2009, 12, 31)),
      (makeTS(2008, 12, 1), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2008, 12, 1),
        makeTS(2009, 12, 31)),
      (makeTS(2006, 12, 1), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2006, 12, 1),
        makeTS(2009, 12, 31))
    ).toDF("followUpEnd", "deathDate", "firstTargetDisease", "trackloss", "observationEnd")

    val expected = Seq(
      (makeTS(2006, 12, 1), "death"),
      (makeTS(2006, 12, 1), "death"),
      (makeTS(2007, 12, 1), "disease"),
      (makeTS(2008, 12, 1), "trackloss"),
      (makeTS(2009, 12, 31), "observationEnd")
    ).toDF("followUpEnd", "endReason")

    // When
    import FollowUpEventsTransformer.FollowUpDataFrame
    val result = input.withEndReason.toDF.select("followUpEnd", "endReason")

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result === expected)
  }

  "withFollowUpPeriod" should "add a column for the follow-up start and one for the end" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "followUpPeriod", makeTS(2006, 1, 1), Some(makeTS(2009, 1, 1))),
      ("Patient_B", "followUpPeriod", makeTS(2006, 2, 1), Some(makeTS(2009, 2, 1))),
      ("Patient_C", "HelloWorld", makeTS(2006, 3, 1), Some(makeTS(2006, 1, 1))),
      ("Patient_C", "followUpPeriod", makeTS(2006, 4, 1), Some(makeTS(2009, 3, 1))),
      ("Patient_D", "HelloWorld", makeTS(2006, 5, 1), None)
    ).toDF("patientID", "category", "start", "end")

    val expected = Seq(
      ("Patient_A", Some(makeTS(2006, 1, 1)), Some(makeTS(2009, 1, 1))),
      ("Patient_B", Some(makeTS(2006, 2, 1)), Some(makeTS(2009, 2, 1))),
      ("Patient_C", Some(makeTS(2006, 4, 1)), Some(makeTS(2009, 3, 1))),
      ("Patient_C", Some(makeTS(2006, 4, 1)), Some(makeTS(2009, 3, 1))),
      ("Patient_D", None, None)
    ).toDF("patientID", "followUpStart", "followUpEnd")

    // When
    import FollowUpEventsTransformer.FollowUpFunctions
    val result = input.withFollowUpPeriodFromEvents.select("patientID", "followUpStart", "followUpEnd")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "transform" should "return a Dataset[FlatEvent] with the follow-up events of each patient" in {
    assert(true)
  }
}
