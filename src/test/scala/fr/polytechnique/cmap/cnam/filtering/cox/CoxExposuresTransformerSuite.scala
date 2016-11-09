package fr.polytechnique.cmap.cnam.filtering.cox

import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.FlatEvent
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class CoxExposuresTransformerSuite extends SharedContext {

  "filterPatients" should "always drop patients who got the disease before the follow up start" +
    "and patients exposed after first year of study if includesDelayedPatients variable is false" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "m1", makeTS(2006, 1, 20), makeTS(2006, 6, 29)),
      ("Patient_A", "molecule", "m2", makeTS(2006, 1, 1), makeTS(2006, 6, 29)),
      ("Patient_A", "molecule", "m3", makeTS(2006, 1, 10), makeTS(2006, 6, 29)),
      ("Patient_A", "disease", "C67", makeTS(2006, 1, 1), makeTS(2006, 6, 29)),
      ("Patient_B", "molecule", "m1", makeTS(2009, 1, 1), makeTS(2009, 6, 30)),
      ("Patient_B", "molecule", "m2", makeTS(2009, 1, 1), makeTS(2009, 6, 30)),
      ("Patient_B", "disease", "m2", makeTS(2009, 1, 1), makeTS(2009, 6, 30)),
      ("Patient_C", "molecule", "m1", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "molecule", "m2", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "disease", "C67", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF("patientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_C", "molecule", "m1"),
      ("Patient_C", "molecule", "m2"),
      ("Patient_C", "disease", "C67")
    ).toDF("patientID", "category", "eventId")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val result = input.filterPatients(includeDelayedPatients = lit(false))
      .select("patientID", "category", "eventId")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  it should "include patients who exposed after first year of study if the includesDelayedPatients " +
    "variable is set to true" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "m1", makeTS(2006, 1, 20), makeTS(2006, 6, 29)),
      ("Patient_A", "molecule", "m2", makeTS(2006, 1, 1), makeTS(2006, 6, 29)),
      ("Patient_A", "molecule", "m3", makeTS(2006, 1, 10), makeTS(2006, 6, 29)),
      ("Patient_A", "disease", "C67", makeTS(2005, 1, 1), makeTS(2006, 6, 29)),
      ("Patient_B", "molecule", "m1", makeTS(2009, 1, 20), makeTS(2009, 6, 29)),
      ("Patient_B", "molecule", "m2", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
      ("Patient_B", "disease", "C67", makeTS(2009, 9, 10), makeTS(2009, 6, 29)),
      ("Patient_B.1", "molecule", "m1", makeTS(2009, 1, 20), makeTS(2009, 6, 29)),
      ("Patient_B.1", "molecule", "m2", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
      ("Patient_B.1", "disease", "C67", makeTS(2009, 5, 10), makeTS(2009, 6, 29)),
      ("Patient_C", "molecule", "m1", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "molecule", "m2", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "disease", "C67", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF("patientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_B", "molecule", "m1"),
      ("Patient_B", "molecule", "m2"),
      ("Patient_B", "disease", "C67"),
      ("Patient_C", "molecule", "m1"),
      ("Patient_C", "molecule", "m2"),
      ("Patient_C", "disease", "C67")
    ).toDF("patientID", "category", "eventId")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val result = input.filterPatients(includeDelayedPatients = lit(true))
      .select("patientID", "category", "eventId")

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
    ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 9, 1), makeTS(2008, 6, 29)),
    ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 10, 1), makeTS(2008, 6, 29)),
    ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
    ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 1, 1))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 1, 1))),
      ("Patient_B", "PIOGLITAZONE", None),
      ("Patient_B", "BENFLUOREX", None)
    ).toDF("PatientID", "eventId", "exposureStart")


    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
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
        "SULFONYLUREA", 900.0, makeTS(2008, 4, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 7, 1), None),
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
        "SULFONYLUREA", 1.0, makeTS(2008, 8, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2006, 8, 1), Some(makeTS(2008, 9, 1)))
    ).toDF

    // When
    val result = CoxExposuresTransformer.transform(input)

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result.toDF === expected)
  }
}


