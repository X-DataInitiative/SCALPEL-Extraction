package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.Columns._
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.mock

class LimitedExposurePeriodAdderSuite extends SharedContext {

  // instance created from a mock DataFrame, to allow testing the InnerImplicits implicit class
  private val mockInstance = new LimitedExposurePeriodAdder(mock(classOf[DataFrame]))

  "withNextDate" should "add a column with the nextDate in a patient-molecule window" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  7, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  3, 1)),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  4, 1))
    ).toDF(PatientID, Value , Start)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), None),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  7, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  7, 1), None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), None),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  4, 1), None)
    ).toDF(PatientID, Value, Start, "nextDate")


    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate

    // Then
    assertDFs(result, expected)
  }

  "getTrackLosses" should "return the correct tracklosses with parameter endThreshold default value" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(5.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(6.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 11, 1)), Some(11.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  3, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None, None)
    ).toDF(PatientID, Value, Start, "nextDate", "delta")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2007,  12, 31), makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  5, 31), makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2007,  12, 31), makeTS(2008,  4, 1))
    ).toDF(PatientID, Value, Start, TracklossDate)

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.getTracklosses()

    // Then
    assertDFs(result, expected)
  }

  "getTrackLosses" should "return the correct tracklosses with parameter endThreshold = 120 days" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2007,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(5.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(6.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 11, 1)), Some(11.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  3, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None, None)
    ).toDF(PatientID, Value, Start, "nextDate", "delta")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2006,  12, 31), makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  5, 31), makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2007,  12, 31), makeTS(2008,  4, 1))
    ).toDF(PatientID, Value, Start, TracklossDate)

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.getTracklosses(endThreshold = 120.days)

    // Then
    assertDFs(result, expected)
  }

  "getTrackLosses" should "return the correct tracklosses with parameter endThreshold = 40 days" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(5.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(2.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(6.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 11, 1)), Some(11.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  3, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None, None)
    ).toDF(PatientID, Value, Start, "nextDate", "delta")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2007,  12, 31), makeTS(2008,  2, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), makeTS(2009,  6, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  5, 31), makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2007,  12, 31), makeTS(2008,  4, 1))
    ).toDF(PatientID, Value, Start, TracklossDate)

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.getTracklosses(endThreshold = 40.days)

    // Then
    assertDFs(result, expected)
  }

  "getTrackLosses" should "return the lines where a trackloss has been identified   endThreshold with days 10" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 8)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 8), Some(makeTS(2009,  1, 1)), Some(8.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  1, 6)), Some(5.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 6), Some(makeTS(2009,  8, 1)), Some(2.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  8, 12)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 12), Some(makeTS(2010,  3, 1)), Some(6.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  3, 2)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 2), None, None)
    ).toDF(PatientID, Value, Start, "nextDate", "delta")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2007,  12, 31), makeTS(2008,  2, 8)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 8), makeTS(2009,  1, 6)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 6), makeTS(2009,  8, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), makeTS(2009,  8, 12)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 12), makeTS(2010,  3, 2))
    ).toDF(PatientID, Value, Start, TracklossDate)

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.getTracklosses(endThreshold = 10.days)

    // Then
    assertDFs(result, expected)
  }


  "withExposureEnd" should "add a column with the end of the exposures (end delay = 0)" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(5.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(6.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 11, 1)), Some(11.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  3, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None, None)
    ).toDF(PatientID, Value, Start, "nextDate", "delta")

    val tracklosses = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2007,  12, 31), makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  5, 31), makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2007,  12, 31), makeTS(2008,  4, 1))
    ).toDF(PatientID, Value, Start, TracklossDate)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)))
    ).toDF(PatientID, Value, Start, ExposureEnd)

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.withExposureEnd(tracklosses)
      .select(PatientID, Value, Start, ExposureEnd)

    // Then
    result.show()
    assertDFs(result, expected)
  }

  "withExposureEnd" should "add a column with the end of the exposures (end Delay = 5 days)" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 11, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  3, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None)
    ).toDF(PatientID, Value, Start, "nextDate")

    val tracklosses = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2007,  12, 13), makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  5, 31), makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2007,  12, 31), makeTS(2008,  4, 1))
    ).toDF(PatientID, Value, Start, TracklossDate)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  5, 6))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 6))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  9, 6))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 6))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 6))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 6))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 6))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  4, 6))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  4, 6))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 6)))
    ).toDF(PatientID, Value, Start, ExposureEnd)

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.withExposureEnd(tracklosses, 5.days)
      .select(PatientID, Value, Start, ExposureEnd)

    // Then
    result.show()
    assertDFs(result, expected)
  }

  "withExposureStart" should "add a column with the start of the exposures (purchases window = 6 months)" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0),
        Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0),
        Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0),
        Some(makeTS(2009,  1, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(5.0),
        Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(1.0),
        Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(1.0),
        Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(6.0),
        Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(1.0),
        Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None, None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0),
        Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 11, 1)), Some(11.0),
        Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(1.0),
        Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None, None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0),
        Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  3, 1)), Some(1.0),
        Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0),
        Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None, None, None)
    ).toDF(PatientID, Value, Start, "nextDate", "delta", ExposureEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), None),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  6, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  3, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None)
    ).toDF(PatientID, Value, Start, ExposureStart)

    // When
    import mockInstance.InnerImplicits
    val resultWithMonths = input.withNextDate.withExposureStart()
      .select(PatientID, Value, Start, ExposureStart)

    val resultwithDays = input.withNextDate.withExposureStart(purchasesWindow = 183.days)
      .select(PatientID, Value, Start, ExposureStart)

    // Then
    assertDFs(resultWithMonths, expected)
    assertDFs(resultwithDays, expected)

  }

  "withExposureStart" should "add a column with the start of the exposures ( purchases window in days 100)" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0),
        Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0),
        Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0),
        Some(makeTS(2009,  1, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(5.0),
        Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(1.0),
        Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(1.0),
        Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(6.0),
        Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(1.0),
        Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None, None)
    ).toDF(PatientID, Value, Start, "nextDate", "delta", ExposureEnd)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), None),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  8, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  8, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None)
    ).toDF(PatientID, Value, Start, ExposureStart)

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.withExposureStart(purchasesWindow = 100.days)
      .select(PatientID, Value, Start, ExposureStart)

    // Then
    assertDFs(result, expected)
  }

  "withStartEnd" should "correctly add exposureStart and exposureEnd with default parameters months" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1))
    ).toDF(PatientID, Value, Start)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  8, 1)), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), None, Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(makeTS(2009, 12, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1)))

    ).toDF(PatientID, Value, Start, ExposureStart, ExposureEnd)

    // When
    val instance = new LimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd()

    // Then
    assertDFs(result, expected)
  }

  "withStartEnd" should "correctly add exposureStart and exposureEnd with default parameters days" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1))
    ).toDF(PatientID, Value, Start)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  8, 1)), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  8, 1)), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), None, Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(makeTS(2009, 12, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1)))
    ).toDF(PatientID, Value, Start, ExposureStart, ExposureEnd)

    // When
    val instance = new LimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(startDelay = 90.days, purchasesWindow = 120.days, endThreshold = Some(120.days))

    // Then
    assertDFs(result, expected)
  }

  "withStartEnd" should "correctly add exposureStart and exposureEnd with default parameters (days changed)" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1))
    ).toDF(PatientID, Value, Start)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1)), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1)), Some(makeTS(2010,  4, 1)))
      //("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None)
    ).toDF(PatientID, Value, Start, ExposureStart, ExposureEnd)

    // When
    val instance = new LimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(purchasesWindow = 50.days, endThreshold = Some(50.days))

    // Then
    assertDFs(result, expected)
  }

  "withStartEnd" should "correctly add exposureStart and exposureEnd with default parameters (days changed minpurchases = 1)" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  8, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  10, 1))
    ).toDF(PatientID, Value, Start)

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  1, 1)), Some(makeTS(2008,  2, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  8, 1), Some(makeTS(2008,  8, 1)), Some(makeTS(2008,  10, 1)))
    ).toDF(PatientID, Value, Start, ExposureStart, ExposureEnd)

    // When
    val instance = new LimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd(purchasesWindow = 100.days, minPurchases = 1, endThreshold = Some(100.days))

    // Then
    assertDFs(result, expected)
  }

}
