package fr.polytechnique.cmap.cnam.etl.old_root.exposures

import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.mock
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

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
    ).toDF("patientID", "eventId" , "start")

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
    ).toDF("patientID", "eventId", "start", "nextDate")


    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate

    // Then
    assertDFs(result, expected)
  }

  "withDelta" should "add a column with the delta in a patient-molecule window" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
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
    ).toDF("PatientID", "eventId" , "start", "nextDate")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1)), Some(3.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1)), Some(8.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(6.0)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), None, None),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  7, 1)), Some(6.0)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  7, 1), None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(1.0)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), None, None),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1)), Some(1.0)),
      ("Patient_B", "SULFONYLUREA", makeTS(2008,  4, 1), None, None)
    ).toDF("PatientID", "eventId", "start", "nextDate", "delta")

    // When
    import mockInstance.InnerImplicits
    val result = input.withDelta

    // Then
    assertDFs(result, expected)
  }

  "getTrackLosses" should "return the lines where a trackloss has been identified (including the first and last lines)" in {
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
    ).toDF("patientID", "eventId", "start", "nextDate", "delta")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), makeTS(2008,  4, 1))
    ).toDF("patientID", "eventId", "start", "tracklossDate")

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.withDelta.getTracklosses()

    // Then
    assertDFs(result, expected)
  }


  "withExposureEnd" should "add a column with the end of the exposures" in {
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
    ).toDF("patientID", "eventId", "start", "nextDate", "delta")

    val tracklosses = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), makeTS(2008,  5, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), makeTS(2009,  1, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), makeTS(2009,  9, 1)),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), makeTS(2010,  4, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), makeTS(2008, 12, 1)),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), makeTS(2009, 12, 1)),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), makeTS(2008,  4, 1))
    ).toDF("patientID", "eventId", "start", "tracklossDate")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), Some(makeTS(2009,  1, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None)
    ).toDF("patientID", "eventId", "start", "exposureEnd")

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.withDelta.withExposureEnd(tracklosses)
      .select("patientID", "eventId", "start", "exposureEnd")

    // Then
    assertDFs(result, expected)
  }

  "withExposureStart" should "add a column with the start of the exposures" in {
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
    ).toDF("patientID", "eventId", "start", "nextDate", "delta", "exposureEnd")

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
    ).toDF("patientID", "eventId", "start", "exposureStart")

    // When
    import mockInstance.InnerImplicits
    val result = input.withNextDate.withDelta.withExposureStart()
      .select("patientID", "eventId", "start", "exposureStart")

    // Then
    assertDFs(result, expected)
  }

  "withStartEnd" should "correctly add exposureStart and exposureEnd with default parameters" in {

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
    ).toDF("patientID", "eventId", "start")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  5, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2008,  5, 1), None, Some(makeTS(2009,  1, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  1, 1), Some(makeTS(2009,  6, 1)), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  6, 1), Some(makeTS(2009,  6, 1)), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  8, 1), Some(makeTS(2009,  6, 1)), Some(makeTS(2009,  9, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2009,  9, 1), Some(makeTS(2010,  3, 1)), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  3, 1), Some(makeTS(2010,  3, 1)), Some(makeTS(2010,  4, 1))),
      ("Patient_A", "PIOGLITAZONE", makeTS(2010,  4, 1), None, None),
      ("Patient_A", "SULFONYLUREA", makeTS(2008,  6, 1), Some(makeTS(2008, 12, 1)), Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2008, 12, 1), Some(makeTS(2009, 12, 1)), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 11, 1), Some(makeTS(2009, 12, 1)), Some(makeTS(2009, 12, 1))),
      ("Patient_A", "SULFONYLUREA", makeTS(2009, 12, 1), None, None),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  1, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  2, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  3, 1), Some(makeTS(2008,  2, 1)), Some(makeTS(2008,  4, 1))),
      ("Patient_B", "PIOGLITAZONE", makeTS(2008,  4, 1), None, None)
    ).toDF("patientID", "eventId", "start", "exposureStart", "exposureEnd")

    // When
    val instance = new LimitedExposurePeriodAdder(input)
    val result = instance.withStartEnd()

    // Then
    assertDFs(result, expected)
  }
}
