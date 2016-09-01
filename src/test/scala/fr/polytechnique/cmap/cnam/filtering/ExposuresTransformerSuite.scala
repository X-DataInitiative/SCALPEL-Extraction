package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames

class ExposuresTransformerSuite extends SharedContext {

  "addFollowUpStart" should "add a column with the start of the follow-up period" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-01-20 00:00:00")),
      ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-01-01 00:00:00")),
      ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-01-10 00:00:00")),
      ("Patient_A", "disease", "Hello World!", Timestamp.valueOf("2007-01-01 00:00:00")),
      ("Patient_B", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2009-01-01 00:00:00")),
      ("Patient_B", "molecule", "BENFLUOREX", Timestamp.valueOf("2007-01-01 00:00:00")),
      ("Patient_B", "disease", "Hello World!", Timestamp.valueOf("2007-01-01 00:00:00"))
    ).toDF("patientID", "category", "eventId", "start")

    val expected = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-01-20 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-01-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-01-10 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "disease", "Hello World!", Timestamp.valueOf("2007-01-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_B", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2009-01-01 00:00:00"), Timestamp.valueOf("2009-06-30 00:00:00")),
      ("Patient_B", "molecule", "BENFLUOREX", Timestamp.valueOf("2007-01-01 00:00:00"), Timestamp.valueOf("2009-06-30 00:00:00")),
      ("Patient_B", "disease", "Hello World!", Timestamp.valueOf("2007-01-01 00:00:00"), Timestamp.valueOf("2009-06-30 00:00:00"))
    ).toDF("patientID", "category", "eventId", "start", "followUpStart")

    // When
    import ExposuresTransformer.ExposuresDataFrame
    val result = input.addFollowUpStart

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  "filterPatients" should "drop patients that we couldn't remove before calculateing follow-up start" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", Timestamp.valueOf("2008-01-20 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "molecule", Timestamp.valueOf("2008-01-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "molecule", Timestamp.valueOf("2008-01-10 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "disease", Timestamp.valueOf("2007-01-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_B", "molecule", Timestamp.valueOf("2009-01-01 00:00:00"), Timestamp.valueOf("2009-06-30 00:00:00")),
      ("Patient_B", "molecule", Timestamp.valueOf("2009-01-01 00:00:00"), Timestamp.valueOf("2009-06-30 00:00:00")),
      ("Patient_B", "disease", Timestamp.valueOf("2009-01-01 00:00:00"), Timestamp.valueOf("2009-06-30 00:00:00")),
      ("Patient_C", "molecule", Timestamp.valueOf("2006-02-01 00:00:00"), Timestamp.valueOf("2006-06-30 00:00:00")),
      ("Patient_C", "molecule", Timestamp.valueOf("2006-01-01 00:00:00"), Timestamp.valueOf("2006-06-30 00:00:00")),
      ("Patient_C", "disease", Timestamp.valueOf("2007-01-01 00:00:00"), Timestamp.valueOf("2006-06-30 00:00:00"))
    ).toDF("patientID", "category", "start", "followUpStart")

    val expected = Seq(
      ("Patient_C", "molecule"),
      ("Patient_C", "molecule"),
      ("Patient_C", "disease")
    ).toDF("patientID", "category")

    // When
    import ExposuresTransformer.ExposuresDataFrame
    val result = input.filterPatients.select("patientID", "category")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  "addFollowUpEnd" should "add a column with the end of the follow-up period" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      // Cancer:
      ("Patient_A", Some(Timestamp.valueOf("2008-01-01 00:00:00")), "molecule", "PIOGLITAZONE", Timestamp.valueOf("2007-12-01 00:00:00")),
      ("Patient_A", Some(Timestamp.valueOf("2008-01-01 00:00:00")), "molecule", "PIOGLITAZONE", Timestamp.valueOf("2007-11-01 00:00:00")),
      ("Patient_A", Some(Timestamp.valueOf("2008-01-01 00:00:00")), "disease", "C67", Timestamp.valueOf("2007-12-01 00:00:00")),
      // Death:
      ("Patient_B", Some(Timestamp.valueOf("2008-01-01 00:00:00")), "molecule", "PIOGLITAZONE", Timestamp.valueOf("2007-12-01 00:00:00")),
      ("Patient_B", Some(Timestamp.valueOf("2008-01-01 00:00:00")), "molecule", "PIOGLITAZONE", Timestamp.valueOf("2007-11-01 00:00:00")),
      // Track loss (to be done):
      // ("Patient_C", None, "molecule", "PIOGLITAZONE", Timestamp.valueOf("2007-01-01 00:00:00")),
      // ("Patient_C", None, "molecule", "PIOGLITAZONE", Timestamp.valueOf("2007-02-01 00:00:00")),
      // End of Observation:
      ("Patient_D", Some(Timestamp.valueOf("2016-01-01 00:00:00")), "molecule", "PIOGLITAZONE", Timestamp.valueOf("2016-01-01 00:00:00")),
      ("Patient_D", Some(Timestamp.valueOf("2016-01-01 00:00:00")), "molecule", "PIOGLITAZONE", Timestamp.valueOf("2016-02-01 00:00:00")),
      ("Patient_D", Some(Timestamp.valueOf("2016-01-01 00:00:00")), "disease", "C67", Timestamp.valueOf("2016-03-01 00:00:00"))
    ).toDF("patientID", "deathDate", "category", "eventId", "start")

    val expected = Seq(
      ("Patient_A", Timestamp.valueOf("2007-12-01 00:00:00")),
      ("Patient_A", Timestamp.valueOf("2007-12-01 00:00:00")),
      ("Patient_A", Timestamp.valueOf("2007-12-01 00:00:00")),
      ("Patient_B", Timestamp.valueOf("2008-01-01 00:00:00")),
      ("Patient_B", Timestamp.valueOf("2008-01-01 00:00:00")),
      // ("Patient_C", Timestamp.valueOf("2007-06-01 00:00:00")),
      // ("Patient_C", Timestamp.valueOf("2007-06-01 00:00:00")),
      ("Patient_D", Timestamp.valueOf("2009-12-31 23:59:59")),
      ("Patient_D", Timestamp.valueOf("2009-12-31 23:59:59")),
      ("Patient_D", Timestamp.valueOf("2009-12-31 23:59:59"))
    ).toDF("patientID", "followUpEnd")

    // When
    import ExposuresTransformer.ExposuresDataFrame
    val result = input.addFollowUpEnd.select("patientID", "followUpEnd")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  "addExposureStart" should "add a column with the start of the exposure" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
    ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-03-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
    ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-01-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
    ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-08-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
    ("Patient_A", "molecule", "GLICLAZIDE", Timestamp.valueOf("2008-09-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
    ("Patient_A", "molecule", "GLICLAZIDE", Timestamp.valueOf("2008-10-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
    ("Patient_B", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2009-01-01 00:00:00"), Timestamp.valueOf("2009-06-29 00:00:00")),
    ("Patient_B", "molecule", "BENFLUOREX", Timestamp.valueOf("2007-01-01 00:00:00"), Timestamp.valueOf("2009-06-29 00:00:00"))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(Timestamp.valueOf("2008-06-29 00:00:00"))),
      ("Patient_A", "PIOGLITAZONE", Some(Timestamp.valueOf("2008-06-29 00:00:00"))),
      ("Patient_A", "PIOGLITAZONE", Some(Timestamp.valueOf("2008-06-29 00:00:00"))),
      ("Patient_A", "GLICLAZIDE", Some(Timestamp.valueOf("2008-12-30 00:00:00"))),
      ("Patient_A", "GLICLAZIDE", Some(Timestamp.valueOf("2008-12-30 00:00:00"))),
      ("Patient_B", "PIOGLITAZONE", None),
      ("Patient_B", "BENFLUOREX", None)
    ).toDF("PatientID", "eventId", "exposureStart")


    // When
    import ExposuresTransformer.ExposuresDataFrame
    val result = input.addExposureStart.select("PatientID", "eventId", "exposureStart")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  "transform" should "return a valid Dataset for a known input" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      FlatEvent("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2007-01-01 00:00:00"), None),
      FlatEvent("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2007-02-01 00:00:00"), None),
      FlatEvent("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2007-05-01 00:00:00"), None),
      FlatEvent("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2007-08-01 00:00:00"), None),
      FlatEvent("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2007-10-01 00:00:00"), None),
      FlatEvent("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "molecule", "GLICLAZIDE", 900.0, Timestamp.valueOf("2008-04-01 00:00:00"), None),
      FlatEvent("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "molecule", "GLICLAZIDE", 900.0, Timestamp.valueOf("2008-05-01 00:00:00"), None),
      FlatEvent("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "molecule", "GLICLAZIDE", 900.0, Timestamp.valueOf("2008-07-01 00:00:00"), None),
      FlatEvent("Patient_B", 1, Timestamp.valueOf("1940-01-01 00:00:00"), Some(Timestamp.valueOf("2008-09-01 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2006-01-01 00:00:00"), None),
      FlatEvent("Patient_B", 1, Timestamp.valueOf("1940-01-01 00:00:00"), Some(Timestamp.valueOf("2008-09-01 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2006-05-01 00:00:00"), None),
      FlatEvent("Patient_B", 1, Timestamp.valueOf("1940-01-01 00:00:00"), Some(Timestamp.valueOf("2008-09-01 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2006-07-01 00:00:00"), None)
    ).toDS

    val expected = Seq(
      Exposure("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "PIOGLITAZONE", Timestamp.valueOf("2007-06-30 00:00:00"), Timestamp.valueOf("2009-07-11 00:00:00")),
      Exposure("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "GLICLAZIDE", Timestamp.valueOf("2008-07-30 00:00:00"), Timestamp.valueOf("2009-07-11 00:00:00")),
      Exposure("Patient_B", 1, Timestamp.valueOf("1940-01-01 00:00:00"), Some(Timestamp.valueOf("2008-09-01 00:00:00")),
        "PIOGLITAZONE", Timestamp.valueOf("2006-07-30 00:00:00"), Timestamp.valueOf("2008-09-01 00:00:00"))
    ).toDF

    // When
    val result = ExposuresTransformer.transform(input)

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result.toDF === expected)
  }
}
