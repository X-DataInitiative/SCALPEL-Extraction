package fr.polytechnique.cmap.cnam.filtering

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS

class ExposuresTransformerSuite extends SharedContext {

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
    import ExposuresTransformer.ExposuresDataFrame
    val result = input.withFollowUpPeriod.select("patientID", "followUpStart", "followUpEnd")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "filterPatients" should "drop patients that we couldn't remove before calculating follow-up start" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", makeTS(2008, 1, 20), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", makeTS(2008, 1, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", makeTS(2008, 1, 10), makeTS(2008, 6, 29)),
      ("Patient_A", "disease", makeTS(2007, 1, 1), makeTS(2008, 6, 29)),
      ("Patient_B", "molecule", makeTS(2009, 1, 1), makeTS(2009, 6, 30)),
      ("Patient_B", "molecule", makeTS(2009, 1, 1), makeTS(2009, 6, 30)),
      ("Patient_B", "disease", makeTS(2009, 1, 1), makeTS(2009, 6, 30)),
      ("Patient_C", "molecule", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "molecule", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "disease", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
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

  "withExposureStart" should "add a column with the start of the exposure" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
    ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 3, 1), makeTS(2008, 6, 29)),
    ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29)),
    ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29)),
    ("Patient_A", "molecule", "SULFONYLUREE", makeTS(2008, 9, 1), makeTS(2008, 6, 29)),
    ("Patient_A", "molecule", "SULFONYLUREE", makeTS(2008, 10, 1), makeTS(2008, 6, 29)),
    ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
    ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29))),
      ("Patient_A", "SULFONYLUREE", Some(makeTS(2009, 1, 1))),
      ("Patient_A", "SULFONYLUREE", Some(makeTS(2009, 1, 1))),
      ("Patient_B", "PIOGLITAZONE", None),
      ("Patient_B", "BENFLUOREX", None)
    ).toDF("PatientID", "eventId", "exposureStart")


    // When
    import ExposuresTransformer.ExposuresDataFrame
    val result = input.withExposureStart.select("PatientID", "eventId", "exposureStart")

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
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "followUpPeriod",
        "death", 900.0, makeTS(2007, 1, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 10, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREE", 900.0, makeTS(2008, 4, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREE", 900.0, makeTS(2008, 5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREE", 900.0, makeTS(2008, 7, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2006, 7, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 1, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 5, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 8, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "SULFONYLUREE", 1.0, makeTS(2008, 8, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2006, 8, 1), Some(makeTS(2008, 9, 1)))
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


