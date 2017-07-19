package fr.polytechnique.cmap.cnam.etl.old_root.exposures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class TimeBasedWeightAggSuite extends SharedContext {

  "aggregateWeight" should "add a weight column with the time based accumulated weight" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  5, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  5, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "PIOGLITAZONE", None, Some(makeTS(2009,  1, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2009,  6, 1)), Some(makeTS(2009,  9, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2009,  6, 1)), Some(makeTS(2009,  9, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2009,  6, 1)), Some(makeTS(2009,  9, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2010,  3, 1)), Some(makeTS(2010,  4, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2010,  3, 1)), Some(makeTS(2010,  4, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "PIOGLITAZONE", None, None, makeTS(2009, 12, 31)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 12, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 12, 1)), Some(makeTS(2009, 12, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 12, 1)), Some(makeTS(2009, 12, 1)), makeTS(2009, 12, 31)),
      ("Patient_A", "SULFONYLUREA", None, None, makeTS(2009, 12, 31)),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1)), makeTS(2008, 12, 31)),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1)), makeTS(2008, 12, 31)),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1)), makeTS(2008, 12, 31)),
      ("Patient_B", "PIOGLITAZONE", None, None, makeTS(2008, 12, 31))
    ).toDF("patientID", "eventId", "exposureStart", "exposureEnd", "followUpEnd")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008,  2, 1)), Some(makeTS(2009, 12, 31)), makeTS(2009, 12, 31), Some(3D)),
      ("Patient_A", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31)), makeTS(2009, 12, 31), None),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2009,  6, 1)), Some(makeTS(2009, 12, 31)), makeTS(2009, 12, 31), Some(6D)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2010,  3, 1)), Some(makeTS(2009, 12, 31)), makeTS(2009, 12, 31), Some(7D)),
      ("Patient_A", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31)), makeTS(2009, 12, 31), None),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 12, 1)), Some(makeTS(2009, 12, 31)), makeTS(2009, 12, 31), Some(0D)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 12, 1)), Some(makeTS(2009, 12, 31)), makeTS(2009, 12, 31), Some(0D)),
      ("Patient_A", "SULFONYLUREA", None, Some(makeTS(2009, 12, 31)), makeTS(2009, 12, 31), None),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2008,  2, 1)), Some(makeTS(2008, 12, 31)), makeTS(2008, 12, 31), Some(2D)),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2008, 12, 31)), makeTS(2008, 12, 31), None)
    ).toDF("patientID", "eventId", "exposureStart", "exposureEnd", "followUpEnd", "weight")

    // When
    val instance = new TimeBasedWeightAgg(input)
    val result = instance.aggregateWeight

    // Then
    assertDFs(expected, result)
 }
}
