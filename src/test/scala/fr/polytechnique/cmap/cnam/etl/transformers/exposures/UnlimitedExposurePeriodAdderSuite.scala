package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.Columns._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class UnlimitedExposurePeriodAdderSuite extends SharedContext {

  private lazy val sqlCtx = sqlContext
  "withStartEnd" should "correctly add exposureStart and exposureEnd with default parameters" in {

    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 2, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 3, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 10, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31))
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 5, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 5, 1)), Some(makeTS(2008, 11, 30))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 5, 1)), Some(makeTS(2008, 11, 30)))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd() // minPurchases = 2, purchasesWindow = 6, startDelay = 3

    // Then
    // The first assert is to make sure the method adds no other columns
    assert(result.columns.diff(input.columns).toList == List(ExposureStart, ExposureEnd))
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
  }

  "withStartEnd" should "correctly find exposures with different parameters" in {

    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 2, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 3, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 10, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31))
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(

      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 11, 30)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 11, 30)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 11, 30)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2009, 6, 29), makeTS(2009, 12, 31)),
      ("Patient_B", "BENFLUOREX", makeTS(2009, 6, 29), makeTS(2009, 12, 31))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance
      .withStartEnd(purchasesWindow = 0.days, startDelay = 0.days, minPurchases = 1) // minPurchases = 2,

    // Then
    // The first assert is to make sure the method adds no other columns
    assert(result.columns.diff(input.columns).toList == List(ExposureStart, ExposureEnd))
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
  }

  it should "correctly add exposureStart and exposureEnd for minPurchases = 1" in {

    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 2, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 3, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 10, 1), makeTS(2007, 6, 29), makeTS(2008, 11, 30)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29), makeTS(2009, 12, 31))
    ).toDF(PatientID, Category, Value, Start, FollowUpStart, FollowUpEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 4, 1), makeTS(2008, 11, 30)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 4, 1), makeTS(2008, 11, 30)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008, 4, 1), makeTS(2008, 11, 30)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2009, 6, 29), makeTS(2009, 12, 31)),
      ("Patient_B", "BENFLUOREX", makeTS(2009, 6, 29), makeTS(2009, 12, 31))
    ).toDF(PatientID, Value, ExposureStart, ExposureEnd)

    // When
    val instance = new UnlimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(minPurchases = 1, startDelay = 3.months) // purchasesWindow = 6, startDelay = 3

    // Then
    assertDFs(expected, result.select(PatientID, Value, ExposureStart, ExposureEnd))
  }
}
