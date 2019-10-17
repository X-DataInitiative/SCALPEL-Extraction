// License: BSD 3 clause

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


  "delayStart" should "delay the start of events by the given period" in {
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
      ("A", "P", makeTS(2008, 1, 11), 1.0),
      ("A", "P", makeTS(2008, 2, 11), 1.0),
      ("A", "P", makeTS(2008, 5, 11), 1.0),
      ("A", "P", makeTS(2008, 6, 11), 1.0),
      ("A", "P", makeTS(2008, 7, 11), 1.0),
      ("A", "P", makeTS(2009, 1, 11), 1.0),
      ("A", "P", makeTS(2009, 7, 11), 1.0),
      ("A", "P", makeTS(2009, 8, 11), 1.0),
      ("A", "S", makeTS(2008, 2, 11), 1.0)
    ).toDF(PatientID, Value, Start, Weight).select(PatientID, Value, Weight, Start)

    val result = mockInstance.delayStart(input, 10.days)

    assertDFs(expected, result)
  }

  "getFirstAndLastPurchase" should "return the first and the last purchase of each potential exposure" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("A", "P", makeTS(2008, 1, 1), 2.0),
      ("A", "P", makeTS(2008, 2, 1), 1.0),
      ("A", "P", makeTS(2008, 5, 1), 1.0),
      ("A", "P", makeTS(2008, 6, 1), 1.0),
      ("A", "P", makeTS(2008, 7, 1), 1.0),
      ("A", "P", makeTS(2009, 1, 1), 1.0),
      ("A", "P", makeTS(2009, 7, 1), 1.0),
      ("A", "P", makeTS(2009, 8, 1), 1.0),
      ("A", "S", makeTS(2008, 2, 1), 2.0)
    ).toDF(PatientID, Value, Start, Weight)

    val expected = Seq(
      ("A", "P", makeTS(2008, 1, 1), 2.0, "first", makeTS(2008, 4, 11)),
      ("A", "P", makeTS(2008, 2, 1), 1.0, "last", makeTS(2008, 3, 11)),
      ("A", "P", makeTS(2008, 5, 1), 1.0, "first", makeTS(2008, 6, 11)),
      ("A", "P", makeTS(2008, 7, 1), 1.0, "last", makeTS(2008, 8, 11)),
      ("A", "P", makeTS(2009, 1, 1), 1.0, "first", makeTS(2009, 2, 11)),
      ("A", "P", makeTS(2009, 7, 1), 1.0, "first", makeTS(2009, 8, 11)),
      ("A", "P", makeTS(2009, 8, 1), 1.0, "last", makeTS(2009, 9, 11)),
      ("A", "S", makeTS(2008, 2, 1), 2.0, "first", makeTS(2008, 5, 11))
    ).toDF(PatientID, Value, Start, Weight, "Status", "purchaseReach")

    val result = mockInstance.getFirstAndLastPurchase(input, 1.months, 3.month, 10.days)

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
      ("A", "P", makeTS(2008, 1, 6), 1.0, makeTS(2008, 1, 6), makeTS(2008, 3, 16)),
      ("A", "P", makeTS(2008, 5, 6), 1.0, makeTS(2008, 5, 6), makeTS(2008, 8, 16)),
      ("A", "P", makeTS(2009, 1, 6), 1.0, makeTS(2009, 1, 6), makeTS(2009, 2, 16)),
      ("A", "P", makeTS(2009, 7, 6), 1.0, makeTS(2009, 7, 6), makeTS(2009, 9, 16)),
      ("A", "S", makeTS(2008, 2, 6), 1.0, makeTS(2008, 2, 6), makeTS(2008, 3, 16))
    ).toDF(PatientID, Value, Start, Weight, ExposureStart, ExposureEnd)

    val result = new LimitedExposurePeriodAdder(input)
      .withStartEnd(0, 5.days, 0.months, Some(1.months), Some(3.months), Some(10.days))

    assertDFs(expected, result)
  }

}