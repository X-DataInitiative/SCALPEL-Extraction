package fr.polytechnique.cmap.cnam.filtering.cox

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.FlatEvent
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class CoxObservationPeriodTransformerSuite extends SharedContext {

  "withObservationStart" should "add a column with the start of the observation period" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 20)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 10)),
      ("Patient_A", "disease", "Hello World!", makeTS(2007, 1, 1)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1)),
      ("Patient_B", "disease", "Hello World!", makeTS(2007, 1, 1))
    ).toDF("patientID", "category", "eventId", "start")

    val expected = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 20), makeTS(2008, 1, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 1, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 10), makeTS(2008, 1, 1)),
      ("Patient_A", "disease", "Hello World!", makeTS(2007, 1, 1), makeTS(2008, 1, 1)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 1, 1)),
      ("Patient_B", "disease", "Hello World!", makeTS(2007, 1, 1), makeTS(2009, 1, 1))
    ).toDF("patientID", "category", "eventId", "start", "observationStart")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxObservationPeriodTransformer.ObservationDataFrame
    val result = input.withObservationStart

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withObservationPeriod" should "add a column for the observation start and one for the end" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "observationPeriod", makeTS(2006, 1, 1), Some(makeTS(2009, 12, 31))),
      ("Patient_B", "observationPeriod", makeTS(2006, 2, 1), Some(makeTS(2009, 12, 31))),
      ("Patient_C", "HelloWorld", makeTS(2006, 3, 1), Some(makeTS(2009, 12, 31))),
      ("Patient_C", "observationPeriod", makeTS(2006, 4, 1), Some(makeTS(2009, 12, 31))),
      ("Patient_D", "HelloWorld", makeTS(2006, 5, 1), None)
    ).toDF("patientID", "category", "start", "end")

    val expected = Seq(
      ("Patient_A", Some(makeTS(2006, 1, 1)), Some(makeTS(2009, 12, 31))),
      ("Patient_B", Some(makeTS(2006, 2, 1)), Some(makeTS(2009, 12, 31))),
      ("Patient_C", Some(makeTS(2006, 4, 1)), Some(makeTS(2009, 12, 31))),
      ("Patient_C", Some(makeTS(2006, 4, 1)), Some(makeTS(2009, 12, 31))),
      ("Patient_D", None, None)
    ).toDF("patientID", "observationStart", "observationEnd")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxObservationPeriodTransformer.ObservationFunctions
    val result = input.withObservationPeriodFromEvents
      .select("patientID", "observationStart", "observationEnd")

    //Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "transform" should "return a Dataset[FlatEvent] with the observation events of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), None, "molecule", "PIOGLITAZONE", 1.0,
        makeTS(2008, 1, 20), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), None, "molecule", "PIOGLITAZONE", 1.0,
        makeTS(2008, 1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), None, "molecule", "PIOGLITAZONE", 1.0,
        makeTS(2008, 1, 10), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), None, "disease", "C67", 1.0,
        makeTS(2007, 1, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1960, 1, 1), None, "molecule", "PIOGLITAZONE", 1.0,
        makeTS(2009, 1, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1960, 1, 1), None, "disease", "C67", 1.0,
        makeTS(2007, 1, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), None, "observationPeriod", "observationPeriod",
        1.0, makeTS(2008, 1, 1), Some(makeTS(2009, 12, 31, 23, 59, 59))),
      FlatEvent("Patient_B", 1, makeTS(1960, 1, 1), None, "observationPeriod", "observationPeriod",
        1.0, makeTS(2009, 1, 1), Some(makeTS(2009, 12, 31, 23, 59, 59)))
    ).toDF

    // When
    import RichDataFrames._
    val result = CoxObservationPeriodTransformer.transform(input)
    result.show
    expected.show
    assert(result.toDF === expected)
  }
}
