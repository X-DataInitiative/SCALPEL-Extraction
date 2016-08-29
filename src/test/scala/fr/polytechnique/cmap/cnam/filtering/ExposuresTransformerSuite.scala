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

  "computeExposureStart" should "return a Dataset of exposures with their start date" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-03-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-01-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2008-08-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "molecule", "GLICLAZIDE", Timestamp.valueOf("2008-09-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "molecule", "GLICLAZIDE", Timestamp.valueOf("2008-10-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "disease", "Hello World!", Timestamp.valueOf("2007-01-01 00:00:00"), Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_B", "molecule", "PIOGLITAZONE", Timestamp.valueOf("2009-01-01 00:00:00"), Timestamp.valueOf("2009-06-29 00:00:00")),
      ("Patient_B", "molecule", "BENFLUOREX", Timestamp.valueOf("2007-01-01 00:00:00"), Timestamp.valueOf("2009-06-29 00:00:00")),
      ("Patient_B", "disease", "Hello World!", Timestamp.valueOf("2007-01-01 00:00:00"), Timestamp.valueOf("2009-06-29 00:00:00"))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Timestamp.valueOf("2008-06-29 00:00:00")),
      ("Patient_A", "GLICLAZIDE", Timestamp.valueOf("2008-12-30 00:00:00"))
    ).toDF("PatientID", "eventId", "exposureStart")


    // When
    import ExposuresTransformer.ExposuresDataFrame
    val result = input.computeExposureStart.select("PatientID", "eventId", "exposureStart")

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
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2008-01-01 00:00:00"), None),
      FlatEvent("Patient_B", 1, Timestamp.valueOf("1940-01-01 00:00:00"), Some(Timestamp.valueOf("2008-09-01 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2008-05-01 00:00:00"), None),
      FlatEvent("Patient_B", 1, Timestamp.valueOf("1940-01-01 00:00:00"), Some(Timestamp.valueOf("2008-09-01 00:00:00")),
        "molecule", "PIOGLITAZONE", 900.0, Timestamp.valueOf("2008-07-01 00:00:00"), None)
    ).toDS

    val expected = Seq(
      Exposure("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "PIOGLITAZONE", Timestamp.valueOf("2007-06-30 00:00:00"), null),
      Exposure("Patient_A", 1, Timestamp.valueOf("1950-01-01 00:00:00"), Some(Timestamp.valueOf("2009-07-11 00:00:00")),
        "GLICLAZIDE", Timestamp.valueOf("2008-07-30 00:00:00"), null),
      Exposure("Patient_B", 1, Timestamp.valueOf("1940-01-01 00:00:00"), Some(Timestamp.valueOf("2008-09-01 00:00:00")),
        "PIOGLITAZONE", Timestamp.valueOf("2008-07-30 00:00:00"), null)
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
