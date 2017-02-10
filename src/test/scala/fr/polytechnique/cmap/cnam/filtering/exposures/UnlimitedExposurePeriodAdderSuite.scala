package fr.polytechnique.cmap.cnam.filtering.exposures

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS

class UnlimitedExposurePeriodAdderSuite extends SharedContext {

  private lazy val sqlCtx = sqlContext
  "withStartEnd" should "correctly add exposureStart and exposureEnd with default parameters" in {

    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 3, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 4, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31))
    ).toDF("patientID", "category", "eventId", "start", "followUpStart", "followUpEnd")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31))),
      ("Patient_B", "BENFLUOREX", None, Some(makeTS(2009, 12, 31)))
    ).toDF("patientID", "eventId", "exposureStart", "exposureEnd")

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd() // minPurchases = 2, purchasesWindow = 6, startDelay = 3

    // Then
    result.show
    expected.show
    import RichDataFrames._
    // The first assert is to make sure the method adds no other columns
    assert(result.columns.diff(input.columns).toList == List("exposureStart", "exposureEnd"))
    assert(expected === result.select("patientID", "eventId", "exposureStart", "exposureEnd"))
  }

  it should "correctly add exposureStart and exposureEnd for minPurchases = 1" in {

    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 3, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 3, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 4, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2009, 6, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart", "followUpEnd")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29)), makeTS(2008, 11, 30)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29)), makeTS(2008, 11, 30)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29)), makeTS(2008, 11, 30)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 6, 1)), makeTS(2008, 11, 30)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 6, 1)), makeTS(2008, 11, 30)),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2009, 6, 29)), makeTS(2009, 12, 31)),
      ("Patient_B", "BENFLUOREX", Some(makeTS(2009, 9, 1)), makeTS(2009, 12, 31))
    ).toDF("patientID", "eventId", "exposureStart", "exposureEnd")

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(minPurchases = 1) // purchasesWindow = 6, startDelay = 3

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(expected === result.select("patientID", "eventId", "exposureStart", "exposureEnd"))
  }

  it should "correctly add exposureStart and exposureEnd for startDelay = 0" in {

    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 3, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 4, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31))
    ).toDF("patientID", "category", "eventId", "start", "followUpStart", "followUpEnd")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 9, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 9, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 9, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 4, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 4, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31))),
      ("Patient_B", "BENFLUOREX", None, Some(makeTS(2009, 12, 31)))
    ).toDF("patientID", "eventId", "exposureStart", "exposureEnd")

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(startDelay = 0) // minPurchases = 1, purchasesWindow = 6

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(expected === result.select("patientID", "eventId", "exposureStart", "exposureEnd"))
  }

  it should "correctly add exposureStart and exposureEnd for purchasesWindow = 9" in {

    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 3, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 4, 1), makeTS(2008, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31))
    ).toDF("patientID", "category", "eventId", "start", "followUpStart", "followUpEnd")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 11, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 11, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 11, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31))),
      ("Patient_B", "BENFLUOREX", None, Some(makeTS(2009, 12, 31)))
    ).toDF("patientID", "eventId", "exposureStart", "exposureEnd")

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(purchasesWindow = 9) // minPurchases = 1, startDelay = 3

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(expected === result.select("patientID", "eventId", "exposureStart", "exposureEnd"))
  }
}
