package fr.polytechnique.cmap.cnam.filtering.ltsccs

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.FlatEvent
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class LTSCCSObservationPeriodTransformerSuite extends SharedContext {

  "transform" should "return a Dataset[FlatEvent] with the follow-up events of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2009,  6, 1), Some(makeTS(2009,  9, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), Some(makeTS(2008,  4, 1)))
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "observationPeriod",
        "observationPeriod", 1.0, makeTS(2008, 2, 1), Some(makeTS(2010, 4, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "observationPeriod",
        "observationPeriod", 1.0, makeTS(2008, 2, 1), Some(makeTS(2008, 4, 1)))
    ).toDS.toDF

    // When
    import RichDataFrames._
    val result = LTSCCSObservationPeriodTransformer.transform(input)
    result.show
    expected.show
    assert(result.toDF === expected)
  }
}
