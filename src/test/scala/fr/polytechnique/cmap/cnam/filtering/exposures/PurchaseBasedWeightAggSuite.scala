package fr.polytechnique.cmap.cnam.filtering.exposures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS

class PurchaseBasedWeightAggSuite extends SharedContext {

  "aggregateWeight" should "add a weight column with the purchase based accumulated weight" in {

    import sqlContext.implicits._

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
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 9, 1), 4.0),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 10), makeTS(2008, 9, 1), 4.0),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 10), makeTS(2008, 10, 1), 5.0),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 7, 1), makeTS(2008, 7, 1), 1.0),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 9, 1), makeTS(2008, 9, 1), 2.0),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 1, 1), 1.0),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2007, 1, 1), 1.0)
    ).toDF("PatientID", "category", "eventId", "start", "exposureStart", "weight")

    // When
    val instance = new PurchaseBasedWeightAgg(input)
    val result = instance.aggregateWeight(Some(makeTS(2006, 1, 1)), Some(1))

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(expected === result)
  }

  it should "add a weight column with the purchase based accumulated weight with cumulativeWindow = 3" in {

    import sqlContext.implicits._

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
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 7, 1), 4.0),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 7, 1), 4.0),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 10), makeTS(2008, 7, 1), 4.0),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 10), makeTS(2008, 10, 1), 5.0),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 7, 1), makeTS(2008, 7, 1), 2.0),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 9, 1), makeTS(2008, 7, 1), 2.0),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 1, 1), 1.0),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2007, 1, 1), 1.0)
    ).toDF("PatientID", "category", "eventId", "start", "exposureStart", "weight")

    // When
    val instance = new PurchaseBasedWeightAgg(input)
    val result = instance.aggregateWeight(Some(makeTS(2006, 1, 1)), Some(3))

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(expected === result)
  }
}
