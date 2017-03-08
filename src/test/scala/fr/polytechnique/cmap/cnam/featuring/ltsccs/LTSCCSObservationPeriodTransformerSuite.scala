package fr.polytechnique.cmap.cnam.featuring.ltsccs

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.old_root.FlatEvent
import fr.polytechnique.cmap.cnam.util.RichDataFrames
import fr.polytechnique.cmap.cnam.util.functions._

class LTSCCSObservationPeriodTransformerSuite extends SharedContext {

  "transform" should "return the correct Observation Period for the trackloss case" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      // Trackloss
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "trackloss",
        "trackloss", 1.0, makeTS(2009,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "trackloss",
        "trackloss", 1.0, makeTS(2009,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2009,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2010,  3, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "observationPeriod",
        "observationPeriod", 1.0, makeTS(2008, 2, 1), Some(makeTS(2009, 6, 1)))
    ).toDS.toDF

    // When
    import RichDataFrames._
    val result = LTSCCSObservationPeriodTransformer.transform(input)
    result.show
    expected.show
    assert(result.toDF === expected)
  }

  it should "return the correct Observation Period for the death date case" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2008, 12, 31)), "trackloss",
        "trackloss", 1.0, makeTS(2009,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2008, 12, 31)), "trackloss",
        "trackloss", 1.0, makeTS(2009,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2008, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2008, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2009,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2008, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2010,  3, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2008, 12, 31)), "observationPeriod",
        "observationPeriod", 1.0, makeTS(2008, 2, 1), Some(makeTS(2008, 12, 31)))
    ).toDS.toDF

    // When
    import RichDataFrames._
    val result = LTSCCSObservationPeriodTransformer.transform(input)
    result.show
    expected.show
    assert(result.toDF === expected)
  }

  it should "return the correct Observation Period for the study end case" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      // Trackloss
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "trackloss",
        "trackloss", 1.0, makeTS(2010,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2009,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2010,  3, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "observationPeriod",
        "observationPeriod", 1.0, makeTS(2008, 2, 1), Some(makeTS(2009, 12, 31, 23, 59, 59)))
    ).toDS.toDF

    // When
    import RichDataFrames._
    val result = LTSCCSObservationPeriodTransformer.transform(input)
    result.show
    expected.show
    assert(result.toDF === expected)
  }

}
