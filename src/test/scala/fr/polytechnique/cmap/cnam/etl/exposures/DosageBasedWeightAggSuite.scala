package fr.polytechnique.cmap.cnam.etl.exposures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.RichDataFrames
import fr.polytechnique.cmap.cnam.util.functions._

class DosageBasedWeightAggSuite extends SharedContext{

  private lazy val sqlCtx = sqlContext

  "aggregateWeight" should "compute exposureStart and weight correctly" in {

    import sqlCtx.implicits._

    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 3, 1), makeTS(2008, 3, 1), 50),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 1, 1), 30),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 8, 1), 100),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 1), makeTS(2008, 10, 1), 200),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 7, 5), makeTS(2008, 7, 5), 100),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 2, 1), makeTS(2008, 2, 1), 40),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 8, 1), makeTS(2008, 8, 1), 150),
      ("Patient_B", "molecule", "SULFONYLUREA", makeTS(2008, 7, 6), makeTS(2008, 7, 6), 40),
      ("Patient_B", "molecule", "SULFONYLUREA", makeTS(2008, 2, 1), makeTS(2008, 2, 1), 140),
      ("Patient_B", "molecule", "SULFONYLUREA", makeTS(2008, 11, 1), makeTS(2008, 11, 1), 150)
    ).toDF("PatientID", "category", "eventId", "start", "exposureStart", "weight")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 1, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 1, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 8, 1)), Some(2.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 10, 1)), Some(4.0)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 2, 1)), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 7, 5)), Some(2.0)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 8, 1)), Some(3.0)),
      ("Patient_B", "SULFONYLUREA", Some(makeTS(2008, 2, 1)), Some(2.0)),
      ("Patient_B", "SULFONYLUREA", Some(makeTS(2008, 2, 1)), Some(2.0)),
      ("Patient_B", "SULFONYLUREA", Some(makeTS(2008, 11, 1)), Some(4.0))
    ).toDF("PatientID", "eventId", "exposureStart", "weight")

    // When
    val dosageLevelIntervals = List(0,140,200,300)
    val instance = new DosageBasedWeightAgg(input)
    val result = instance.aggregateWeight(None,None,None,None,Some(dosageLevelIntervals))
      .select("PatientID", "eventId", "exposureStart", "weight")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  "aggregateWeight" should "compute exposureStart and weight correctly for dosage-level-intervals = [0]" in {

    import sqlCtx.implicits._

    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 3, 1), makeTS(2008, 3, 1), 50),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 1, 1), 30),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 8, 1), 100),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 1), makeTS(2008, 10, 1), 200),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 7, 5), makeTS(2008, 7, 5), 100),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 2, 1), makeTS(2008, 2, 1), 40),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 8, 1), makeTS(2008, 8, 1), 150),
      ("Patient_B", "molecule", "SULFONYLUREA", makeTS(2008, 7, 6), makeTS(2008, 7, 6), 40),
      ("Patient_B", "molecule", "SULFONYLUREA", makeTS(2008, 2, 1), makeTS(2008, 2, 1), 140),
      ("Patient_B", "molecule", "SULFONYLUREA", makeTS(2008, 11, 1), makeTS(2008, 11, 1), 150)
    ).toDF("PatientID", "category", "eventId", "start", "exposureStart","weight")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 1, 1), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 1, 1), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 1, 1), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 1, 1), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 2, 1), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 2, 1), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 2, 1), Some(1.0)),
      ("Patient_B", "SULFONYLUREA", makeTS(2008, 2, 1), Some(1.0)),
      ("Patient_B", "SULFONYLUREA", makeTS(2008, 2, 1), Some(1.0)),
      ("Patient_B", "SULFONYLUREA", makeTS(2008, 2, 1), Some(1.0))
    ).toDF("PatientID", "eventId", "exposureStart", "weight")

    // When

    val dosageLevelIntervals = List(0)
    val instance = new DosageBasedWeightAgg(input)
    val result = instance.aggregateWeight(None,None,None,None,Some(dosageLevelIntervals))
      .select("PatientID", "eventId", "exposureStart", "weight")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  "aggregateWeight" should "compute exposureStart and weight correctly if there are duplicate exposureStart" in {

    import sqlCtx.implicits._
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 3, 1), makeTS(2008, 3, 1), 75),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 8, 1), 50),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 8, 1), 100),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 1), makeTS(2008, 10, 1), 200)

    ).toDF("PatientID", "category", "eventId", "start", "exposureStart","weight")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 3, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 8, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 8, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 10, 1)), Some(4.0))
    ).toDF("PatientID", "eventId", "exposureStart", "weight")

    // When
    val dosageLevelIntervals = List(0,140,200,300)
    val instance = new DosageBasedWeightAgg(input)
    val result = instance.aggregateWeight(None,None,None,None,Some(dosageLevelIntervals))
      .select("PatientID", "eventId", "exposureStart", "weight")
    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }
}
