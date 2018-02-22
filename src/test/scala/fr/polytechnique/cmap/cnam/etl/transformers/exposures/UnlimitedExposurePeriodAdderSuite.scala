package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.Columns._
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._

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
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31))),
      ("Patient_B", "BENFLUOREX", None, Some(makeTS(2009, 12, 31)))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd() // minPurchases = 2, purchasesWindow = 6, startDelay = 3

    // Then
    // The first assert is to make sure the method adds no other columns
    assert(result.columns.diff(input.columns).toList == List(ExposureStart, ExposureEnd))
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
 }

  "withStartEnd" should "correctly add exposureStart and exposureEnd with default parameters in days" in {

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
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31))),
      ("Patient_B", "BENFLUOREX", None, Some(makeTS(2009, 12, 31)))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(purchasesWindow = 184.days, startDelay = 91.days) // minPurchases = 2,

    // Then
    // The first assert is to make sure the method adds no other columns
    assert(result.columns.diff(input.columns).toList == List(ExposureStart, ExposureEnd))
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
  }

  "withStartEnd" should "correctly add exposureStart and exposureEnd with default parameters in days changed" in {

    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 3), makeTS(2008, 1, 3), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 1, 3), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 9), makeTS(2008, 1, 3), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 1, 3), makeTS(2008, 1, 3), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 1, 4), makeTS(2008, 1, 3), makeTS(2008, 11, 30)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2009, 1, 3), makeTS(2009, 12, 31)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 1, 3), makeTS(2009, 12, 31))
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 1, 8)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 1, 8)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 1, 8)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 1, 9)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 1, 9)), Some(makeTS(2008, 11, 30))),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31))),
      ("Patient_B", "BENFLUOREX", None, Some(makeTS(2009, 12, 31)))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(purchasesWindow = 20.days, startDelay = 5.days) // minPurchases = 2,

    // Then
    // The first assert is to make sure the method adds no other columns
    assert(result.columns.diff(input.columns).toList == List(ExposureStart, ExposureEnd))
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
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
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29)), makeTS(2008, 11, 30)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29)), makeTS(2008, 11, 30)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29)), makeTS(2008, 11, 30)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 6, 1)), makeTS(2008, 11, 30)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 6, 1)), makeTS(2008, 11, 30)),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2009, 6, 29)), makeTS(2009, 12, 31)),
      ("Patient_B", "BENFLUOREX", Some(makeTS(2009, 9, 1)), makeTS(2009, 12, 31))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(minPurchases = 1) // purchasesWindow = 6, startDelay = 3

    // Then
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
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
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 9, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 9, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 9, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 4, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 4, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31))),
      ("Patient_B", "BENFLUOREX", None, Some(makeTS(2009, 12, 31)))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(startDelay = 0.months) // minPurchases = 1, purchasesWindow = 6

    // Then
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
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
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 11, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 11, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 11, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31))),
      ("Patient_B", "BENFLUOREX", None, Some(makeTS(2009, 12, 31)))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(purchasesWindow = 9.months) // minPurchases = 1, startDelay = 3

    // Then
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
 }

  it should "correctly add exposureStart and exposureEnd for purchasesWindow = 180 days" in {

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
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 11, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 11, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 11, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_B", "PIOGLITAZONE", None, Some(makeTS(2009, 12, 31))),
      ("Patient_B", "BENFLUOREX", None, Some(makeTS(2009, 12, 31)))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(purchasesWindow = 270.days) // minPurchases = 1, startDelay = 3

    // Then
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
  }
}
