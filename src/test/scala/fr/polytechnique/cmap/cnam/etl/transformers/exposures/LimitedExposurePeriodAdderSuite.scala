package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.implicits.IntImplicits
import org.mockito.Mockito.mock
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.Columns._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class LimitedExposurePeriodAdderSuite extends SharedContext {

  // instance created from a mock DataFrame, to allow testing the InnerImplicits implicit class
  private val mockInstance = new LimitedExposurePeriodAdder(mock(classOf[DataFrame]))

  "getFirstAndLastPurchase" should "return the first and the last purchase of each potential exposure" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("A", "P", makeTS(2008, 1, 1), 1.0),
      ("A", "P", makeTS(2008, 2, 1), 1.0),
      ("A", "P", makeTS(2008, 5, 1), 1.0),
      ("A", "P", makeTS(2008, 6, 1), 1.0),
      ("A", "P", makeTS(2008, 7, 1), 1.0),
      ("A", "P", makeTS(2009, 1, 1), 1.0),
      ("A", "P", makeTS(2009, 7, 1), 1.0),
      ("A", "P", makeTS(2009, 8, 1), 1.0),
      ("A", "S", makeTS(2008, 2, 1), 1.0)
    ).toDF(PatientID, Value, Start, Weight)

    val expected = Seq(
      ("A", "P", makeTS(2008, 1, 1), 1.0, "first", makeTS(2008, 2, 1)),
      ("A", "P", makeTS(2008, 2, 1), 1.0, "last", makeTS(2008, 3, 1)),
      ("A", "P", makeTS(2008, 5, 1), 1.0, "first", makeTS(2008, 6, 1)),
      ("A", "P", makeTS(2008, 7, 1), 1.0, "last", makeTS(2008, 8, 1)),
      ("A", "P", makeTS(2009, 1, 1), 1.0, "first", makeTS(2009, 2, 1)),
      ("A", "P", makeTS(2009, 7, 1), 1.0, "first", makeTS(2009, 8, 1)),
      ("A", "P", makeTS(2009, 8, 1), 1.0, "last", makeTS(2009, 9, 1)),
      ("A", "S", makeTS(2008, 2, 1), 1.0, "first", makeTS(2008, 3, 1))
    ).toDF(PatientID, Value, Start, Weight, "Status", "purchaseReach")

    val result = mockInstance.getFirstAndLastPurchase(input, 1.month, 3.months)

    assertDFs(expected, result)
  }

  "toExposure" should "transform first and last purchases DataFrame to Exposures" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = Seq(
      ("A", "P", makeTS(2008, 1, 1), 1.0, "first", makeTS(2008, 2, 1)),
      ("A", "P", makeTS(2008, 2, 1), 1.0, "last", makeTS(2008, 3, 1)),
      ("A", "P", makeTS(2008, 5, 1), 1.0, "first", makeTS(2008, 6, 1)),
      ("A", "P", makeTS(2008, 7, 1), 1.0, "last", makeTS(2008, 8, 1)),
      ("A", "P", makeTS(2009, 1, 1), 1.0, "first", makeTS(2009, 2, 1)),
      ("A", "P", makeTS(2009, 7, 1), 1.0, "first", makeTS(2009, 8, 1)),
      ("A", "P", makeTS(2009, 8, 1), 1.0, "last", makeTS(2009, 9, 1)),
      ("A", "S", makeTS(2008, 2, 1), 1.0, "first", makeTS(2008, 3, 1))
    ).toDF(PatientID, Value, Start, Weight, "Status", "purchaseReach")

    val expected = Seq(
      ("A", "P", makeTS(2008, 1, 1), 1.0, makeTS(2008, 3, 1), makeTS(2008, 1, 1)),
      ("A", "P", makeTS(2008, 5, 1), 1.0, makeTS(2008, 8, 1), makeTS(2008, 5, 1)),
      ("A", "P", makeTS(2009, 1, 1), 1.0, makeTS(2009, 2, 1), makeTS(2009, 1, 1)),
      ("A", "P", makeTS(2009, 7, 1), 1.0, makeTS(2009, 9, 1), makeTS(2009, 7, 1)),
      ("A", "S", makeTS(2008, 2, 1), 1.0, makeTS(2008, 3, 1), makeTS(2008, 2, 1))
    ).toDF(PatientID, Value, Start, Weight, ExposureEnd, ExposureStart)

    val result = mockInstance.toExposure(input)

    assertDFs(expected, result)
  }

  "withStartEnd" should "transform drug purchases into exposures" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("A", "P", makeTS(2008, 1, 1), 1.0),
      ("A", "P", makeTS(2008, 2, 1), 1.0),
      ("A", "P", makeTS(2008, 5, 1), 1.0),
      ("A", "P", makeTS(2008, 6, 1), 1.0),
      ("A", "P", makeTS(2008, 7, 1), 1.0),
      ("A", "P", makeTS(2009, 1, 1), 1.0),
      ("A", "P", makeTS(2009, 7, 1), 1.0),
      ("A", "P", makeTS(2009, 8, 1), 1.0),
      ("A", "S", makeTS(2008, 2, 1), 1.0)
    ).toDF(PatientID, Value, Start, Weight)

    val expected = Seq(
      ("A", "P", makeTS(2008, 1, 1), 1.0, makeTS(2008, 1, 1), makeTS(2008, 3, 1)),
      ("A", "P", makeTS(2008, 5, 1), 1.0, makeTS(2008, 5, 1), makeTS(2008, 8, 1)),
      ("A", "P", makeTS(2009, 1, 1), 1.0, makeTS(2009, 1, 1), makeTS(2009, 2, 1)),
      ("A", "P", makeTS(2009, 7, 1), 1.0, makeTS(2009, 7, 1), makeTS(2009, 9, 1)),
      ("A", "S", makeTS(2008, 2, 1), 1.0, makeTS(2008, 2, 1), makeTS(2008, 3, 1))
    ).toDF(PatientID, Value, Start, Weight, ExposureStart, ExposureEnd)

    val result = new LimitedExposurePeriodAdder(input)
      .withStartEnd(0, 0.months, 0.months, Some(1.months), Some(3.months), Some(0.months))

    assertDFs(expected, result)
  }

}