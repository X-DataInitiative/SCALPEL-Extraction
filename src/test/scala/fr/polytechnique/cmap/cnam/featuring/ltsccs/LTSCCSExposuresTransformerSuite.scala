package fr.polytechnique.cmap.cnam.featuring.ltsccs

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.old_root.FlatEvent
import fr.polytechnique.cmap.cnam.util.RichDataFrames
import fr.polytechnique.cmap.cnam.util.functions._

class LTSCCSExposuresTransformerSuite extends SharedContext {

  "withNextDate" should "add a column with the nextDate in a patient-molecule window" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  7, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  3, 1)),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  4, 1))
    ).toDF("PatientID", "moleculeName" , "eventDate")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), None),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  7, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  7, 1), None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), None),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  4, 1), None)
    ).toDF("PatientID", "moleculeName", "eventDate", "nextDate")


    // When
    import LTSCCSExposuresTransformer.LTSCCSExposuresDataFrame
    val result = input.withNextDate

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withDelta" should "add a column with the delta in a patient-molecule window" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), None),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  7, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  7, 1), None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), None),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  4, 1), None)
    ).toDF("PatientID", "moleculeName" , "eventDate", "nextDate")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), None, None),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  7, 1)), Some(6.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  7, 1), None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), None, None),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0)),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  4, 1), None, None)
    ).toDF("PatientID", "moleculeName", "eventDate", "nextDate", "delta")

    // When
    import LTSCCSExposuresTransformer.LTSCCSExposuresDataFrame
    val result = input.withDelta

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "filterPatients" should "remove unwanted patients" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2009, 10, 1), makeTS(2009, 10, 1)),
      ("Patient_A", "molecule", "", makeTS(2009, 11, 1), makeTS(2009, 10, 1)),
      ("Patient_A", "disease",  "C67", makeTS(2009, 12, 1), makeTS(2009, 10, 1)),
      ("Patient_B", "molecule", "", makeTS(2008,  4, 1), makeTS(2008,  4, 1)),
      ("Patient_B", "molecule", "", makeTS(2008,  4, 1), makeTS(2008,  4, 1))
    ).toDF("patientID", "category", "eventId", "start", "observationStart")
    val expected = Seq(
      ("Patient_B", "molecule", "", makeTS(2008,  4, 1), makeTS(2008,  4, 1)),
      ("Patient_B", "molecule", "", makeTS(2008,  4, 1), makeTS(2008,  4, 1))
    ).toDF("patientID", "category", "eventId", "start", "observationStart")

    // When
    import LTSCCSExposuresTransformer.LTSCCSExposuresDataFrame
    val result = input.filterPatients

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "getTrackLosses" should "return the lines where a trackloss has been identified (including the first and last lines)" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(5.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(6.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 11, 1)), Some(11.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  3, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None, None)
    ).toDF("patientID", "moleculeName", "eventDate", "nextDate", "delta")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), makeTS(2008,  4, 1))
    ).toDF("patientID", "moleculeName", "eventDate", "tracklossDate")

    // When
    import LTSCCSExposuresTransformer.LTSCCSExposuresDataFrame
    val result = input.withNextDate.withDelta.getTracklosses

    // Then
    import RichDataFrames._
    result.orderBy("patientID", "moleculeName", "eventDate").show
    expected.orderBy("patientID", "moleculeName", "eventDate").show
    assert(result === expected)
  }

  "withExposureEnd" should "add a column with the end of the exposures" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(5.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(6.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 11, 1)), Some(11.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  3, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None, None)
    ).toDF("patientID", "moleculeName", "eventDate", "nextDate", "delta")

    val tracklosses = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), makeTS(2008,  4, 1))
    ).toDF("patientID", "moleculeName", "eventDate", "tracklossDate")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None)
    ).toDF("patientID", "moleculeName", "eventDate", "exposureEnd")

    // When
    import LTSCCSExposuresTransformer.LTSCCSExposuresDataFrame
    val result = input.withNextDate.withDelta.withExposureEnd(tracklosses)
      .select("patientID", "moleculeName", "eventDate", "exposureEnd")

    // Then
    import RichDataFrames._
    result.orderBy("patientID", "moleculeName", "eventDate").show
    expected.orderBy("patientID", "moleculeName", "eventDate").show
    assert(result === expected)
  }

  "withExposureStart" should "add a column with the start of the exposures" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0),
        Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0),
        Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0),
        Some(makeTS(2009,  1, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(5.0),
        Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(1.0),
        Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(1.0),
        Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(6.0),
        Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(1.0),
        Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None, None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0),
        Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 11, 1)), Some(11.0),
        Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(1.0),
        Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None, None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0),
        Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  3, 1)), Some(1.0),
        Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0),
        Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None, None, None)
    ).toDF("patientID", "moleculeName", "eventDate", "nextDate", "delta", "exposureEnd")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), None),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  3, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None)
    ).toDF("patientID", "moleculeName", "eventDate", "exposureStart")

    // When
    import LTSCCSExposuresTransformer.LTSCCSExposuresDataFrame
    val result = input.withNextDate.withDelta.withExposureStart
      .select("patientID", "moleculeName", "eventDate", "exposureStart")

    // Then
    import RichDataFrames._
    result.orderBy("patientID", "moleculeName", "eventDate").show
    expected.orderBy("patientID", "moleculeName", "eventDate").show
    assert(result === expected)
  }

  "transform" should "return a valid Dataset for a known input" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "observationPeriod",
        "observationPeriod", 1.0, makeTS(2008,  1, 1), Some(makeTS(2011, 12, 31))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2009,  1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2009,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2009,  8, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2009,  9, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2010,  3, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2010,  4, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008, 12, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2009, 11, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2009, 12, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "observationPeriod",
        "observationPeriod", 1.0, makeTS(2008,  1, 1), Some(makeTS(2011, 12, 31))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  1, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  3, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  4, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "observationPeriod",
        "observationPeriod", 1.0, makeTS(2008,  1, 1), Some(makeTS(2011, 12, 31))),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "disease",
        "C67", 1.0, makeTS(2008,  4, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  1, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None),
        FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  3, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2009,  6, 1), Some(makeTS(2009,  9, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), Some(makeTS(2008,  4, 1)))
    ).toDF

    // When
    val result = LTSCCSExposuresTransformer.transform(input)

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result.toDF === expected)
  }
}


