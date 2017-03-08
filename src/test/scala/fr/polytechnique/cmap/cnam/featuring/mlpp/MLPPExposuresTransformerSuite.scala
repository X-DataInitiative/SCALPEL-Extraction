package fr.polytechnique.cmap.cnam.featuring.mlpp

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.old_root.{FilteringConfig, FlatEvent}
import fr.polytechnique.cmap.cnam.util.RichDataFrames
import fr.polytechnique.cmap.cnam.util.functions._

class MLPPExposuresTransformerSuite extends SharedContext {

  "filterDelayedEntries" should "keep only patients who purchased a medicine in the first year of the study" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2008, 1, 1)),
      ("Patient_A", "molecule", "", makeTS(2008, 2, 1)),
      ("Patient_B", "molecule", "", makeTS(2009, 1, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 2, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 1, 1))
    ).toDF("patientID", "category", "eventId", "start")

    val expected = Seq(
      ("Patient_C", "molecule"),
      ("Patient_C", "molecule")
    ).toDF("patientID", "category")

    // When
    import MLPPExposuresTransformer.ExposuresDataFrame
    val result = input.filterDelayedEntries(true).select("patientID", "category")

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  it should "return the same data if we pass false" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2008, 1, 1)),
      ("Patient_B", "molecule", "", makeTS(2009, 1, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 1, 1))
    ).toDF("patientID", "category", "eventId", "start")

    val expected = input

    // When
    import MLPPExposuresTransformer.ExposuresDataFrame
    val result = input.filterDelayedEntries(false)

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  "filterDiagnosedPatients" should "keep only patients who did not have a target disease before the study start (+ threshold)" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2008, 1, 10)),
      ("Patient_A", "disease", "targetDisease", makeTS(2005, 1, 1)),
      ("Patient_B", "molecule", "", makeTS(2009, 1, 1)),
      ("Patient_B", "disease", "targetDisease", makeTS(2006, 8, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 1, 1))
    ).toDF("patientID", "category", "eventId", "start")

    val expected = Seq(
      ("Patient_B", "molecule"),
      ("Patient_B", "disease"),
      ("Patient_C", "molecule")
    ).toDF("patientID", "category")

    // When
    import MLPPExposuresTransformer.ExposuresDataFrame
    val result = input.filterEarlyDiagnosedPatients(true).select("patientID", "category")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  it should "return the same data if we pass false" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2008, 1, 10)),
      ("Patient_A", "disease", "targetDisease", makeTS(2007, 1, 1))
    ).toDF("patientID", "category", "eventId", "start")

    val expected = input

    // When
    import MLPPExposuresTransformer.ExposuresDataFrame
    val result = input.filterEarlyDiagnosedPatients(false)

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  "filterLostPatients" should "remove patients when they have a trackloss events" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", makeTS(2006, 1, 1)),
      ("Patient_A", "molecule", makeTS(2006, 2, 1)),
      ("Patient_B", "molecule", makeTS(2006, 5, 1)),
      ("Patient_B", "trackloss", makeTS(2007, 1, 1)),
      ("Patient_C", "molecule", makeTS(2006, 11, 1))
    ).toDF("patientID", "category", "start")

    val expected = Seq(
      ("Patient_A", "molecule", makeTS(2006, 1, 1)),
      ("Patient_A", "molecule", makeTS(2006, 2, 1)),
      ("Patient_C", "molecule", makeTS(2006, 11, 1))
    ).toDF("patientID", "category", "start")

    // When
    import MLPPExposuresTransformer.ExposuresDataFrame
    val result = input.filterLostPatients(true)

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  "filterNeverSickPatients" should "remove patients who never have a target disease" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2006, 1, 1)),
      ("Patient_A", "molecule", "", makeTS(2006, 3, 1)),
      ("Patient_A", "disease", "targetDisease", makeTS(2006, 6, 1)),
      ("Patient_B", "molecule", "", makeTS(2006, 5, 1)),
      ("Patient_B", "molecule", "", makeTS(2007, 1, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 11, 1))
    ).toDF("patientID", "category", "eventId", "start")

    val expected = Seq(
      ("Patient_A", "molecule", "", makeTS(2006, 1, 1)),
      ("Patient_A", "molecule", "", makeTS(2006, 3, 1)),
      ("Patient_A", "disease", "targetDisease", makeTS(2006, 6, 1))
    ).toDF("patientID", "category", "eventId", "start")

    // When
    import MLPPExposuresTransformer.ExposuresDataFrame
    val result = input.filterNeverSickPatients(true)

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withExposureStart" should "add a column with the start of the default MLPP exposure definition" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 2, 1)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 3, 1)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2008, 4, 1)),
      ("Patient_B", "molecule",   "BENFLUOREX", makeTS(2008, 5, 1))
    ).toDF("PatientID", "category", "eventId", "start")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 1, 1))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 2, 1))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 3, 1))),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2008, 4, 1))),
      ("Patient_B",   "BENFLUOREX", Some(makeTS(2008, 5, 1)))
    ).toDF("PatientID", "eventId", "exposureStart")

    // When
    import MLPPExposuresTransformer.ExposuresDataFrame
    val result = input.withExposureStart(minPurchases = 1, firstOnly = false)
      .select("PatientID", "eventId", "exposureStart")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  it should "add a column with the start of the exposure, using a 'cox-like' definition" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008,  6, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008,  8, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 11, 1)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008,  9, 1)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 10, 1)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009,  1, 1))
    ).toDF("PatientID", "category", "eventId", "start")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008,  6, 1))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 10, 1))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 10, 1))),
      ("Patient_B", "PIOGLITAZONE", None)
    ).toDF("PatientID", "eventId", "exposureStart")


    // When
    import MLPPExposuresTransformer.ExposuresDataFrame
    val result = input.withExposureStart(
      minPurchases = 2, intervalSize = 6, startDelay = 0, firstOnly = true
    ).select("PatientID", "eventId", "exposureStart")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "transform" should "return a valid Dataset for a known input" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), None, "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 1, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), None, "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 5, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), None, "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2006, 1, 1), Some(makeTS(2006, 1, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 2, 1), Some(makeTS(2007, 2, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), Some(makeTS(2007, 5, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), None, "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2006, 1, 1), Some(makeTS(2006, 1, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), None, "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2006, 5, 1), Some(makeTS(2006, 5, 1)))
    ).toDS.toDF

    // When
    val result = MLPPExposuresTransformer.transform(input)

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result.toDF === expected)
  }
}
