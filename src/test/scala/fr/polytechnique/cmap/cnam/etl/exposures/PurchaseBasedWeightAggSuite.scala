package fr.polytechnique.cmap.cnam.etl.exposures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.RichDataFrames
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class PurchaseBasedWeightAggSuite extends SharedContext {

  "aggregateWeight" should "add a weight column with the purchase based accumulated weight" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 1, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 8, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 9, 1)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 10), makeTS(2008, 9, 10)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 10), makeTS(2008, 10, 10)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 7, 1), makeTS(2008, 7, 1)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 9, 1), makeTS(2008, 9, 1)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 1, 1)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2007, 1, 1))
    ).toDF("PatientID", "category", "eventId", "start", "exposureStart")

    val expected = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 1, 1), 1.0),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 8, 1), 2.0),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 8, 1), 2.0),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 10), makeTS(2008, 9, 10), 3.0),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 10), makeTS(2008, 9, 10), 3.0),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 7, 1), makeTS(2008, 7, 1), 1.0),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 9, 1), makeTS(2008, 9, 1), 2.0),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 1, 1), 1.0),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2007, 1, 1), 1.0)
    ).toDF("PatientID", "category", "eventId", "start", "exposureStart", "weight")

    // When
    val instance = new PurchaseBasedWeightAgg(input)
    val result = instance.aggregateWeight(purchaseIntervals = Some(List(0, 2, 4)))

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(expected === result)
  }
}
