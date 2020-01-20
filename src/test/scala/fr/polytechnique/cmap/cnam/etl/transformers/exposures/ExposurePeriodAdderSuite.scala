// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql.Dataset
import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event, Exposure, FollowUp}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class ExposurePeriodAdderSuite extends SharedContext{
  "toExposure" should "transform drugs to exposure based on the limited adder strategy" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given

    val input: Dataset[Event[Drug]] = Seq(
      Drug("patient", "Antidepresseurs", 2, makeTS(2014, 1, 8)),
      Drug("patient", "Antidepresseurs", 2, makeTS(2014, 2, 5)),
      Drug("patient", "Antidepresseurs", 2, makeTS(2014, 3, 12)),
      Drug("patient", "Antidepresseurs", 2, makeTS(2014, 4, 20)),
      Drug("patient", "Antidepresseurs", 2, makeTS(2014, 6, 3))
    ).toDS
    val followUp: Dataset[Event[FollowUp]] = Seq(
      FollowUp("patient", "any_reason", makeTS(2006, 6, 1), makeTS(2020, 12, 31)),
      FollowUp("Patient_B", "any_reason", makeTS(2006, 7, 1), makeTS(2006, 7, 1)),
      FollowUp("Patient_C", "any_reason", makeTS(2016, 8, 1), makeTS(2009, 12, 31))
    ).toDS()

    val expected: Dataset[Event[Exposure]] = Seq[Event[Exposure]](
      Exposure("patient", "NA", "Antidepresseurs", 1, makeTS(2014, 1, 8), Some(makeTS(2014, 6, 7)))
    ).toDS()
    val exposureAdder = LimitedExposureAdder(0.days, 15.days, 90.days, 30.days, PurchaseCountBased)

    val result = exposureAdder.toExposure(followUp)(input)
    assertDSs(result, expected)
  }

  "toExposure" should "transform drugs to exposure based on the unlimited adder strategy" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given

    val input: Dataset[Event[Drug]] = Seq[Event[Drug]](
      Drug("Patient_A", "PIOGLITAZONE", 1, makeTS(2008, 1, 1)),
      Drug("Patient_A", "PIOGLITAZONE", 1, makeTS(2008, 2, 1)),
      Drug("Patient_A", "PIOGLITAZONE", 1, makeTS(2008, 9, 1)),
      Drug("Patient_A", "SULFONYLUREA", 1, makeTS(2009, 3, 1)),
      Drug("Patient_A", "SULFONYLUREA", 1, makeTS(2009, 10, 1)),
      Drug("Patient_B", "PIOGLITAZONE", 1, makeTS(2009, 1, 1)),
      Drug("Patient_B", "BENFLUOREX", 1, makeTS(2007, 1, 1))
    ).toDS

    val followUp: Dataset[Event[FollowUp]] = Seq[Event[FollowUp]](
      FollowUp("Patient_A", "any_reason", makeTS(2006, 6, 1), makeTS(2008, 11, 30)),
      FollowUp("Patient_B", "any_reason", makeTS(2006, 7, 1), makeTS(2006, 7, 1)),
      FollowUp("Patient_C", "any_reason", makeTS(2016, 8, 1), makeTS(2009, 12, 31))
    ).toDS()

    val expected: Dataset[Event[Exposure]] = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "PIOGLITAZONE", 1, makeTS(2008, 2, 1), Some(makeTS(2008, 11, 30)))
    ).toDS()
    val exposureAdder = UnlimitedExposureAdder(3.months, 2, 6.months)

    val result = exposureAdder.toExposure(followUp)(input)
    assertDSs(result, expected)
  }

  it should "transform drugs to exposure based on the unlimited adder strategy with different parameters" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given

    val input: Dataset[Event[Drug]] = Seq[Event[Drug]](
      Drug("Patient_A", "PIOGLITAZONE", 1, makeTS(2008, 1, 1)),
      Drug("Patient_A", "PIOGLITAZONE", 1, makeTS(2008, 2, 1)),
      Drug("Patient_A", "PIOGLITAZONE", 1, makeTS(2008, 9, 1)),
      Drug("Patient_A", "SULFONYLUREA", 1, makeTS(2009, 3, 1)),
      Drug("Patient_A", "SULFONYLUREA", 1, makeTS(2009, 10, 1)),
      Drug("Patient_B", "PIOGLITAZONE", 1, makeTS(2009, 1, 1)),
      Drug("Patient_B", "BENFLUOREX", 1, makeTS(2007, 1, 1))
    ).toDS

    val followUp: Dataset[Event[FollowUp]] = Seq[Event[FollowUp]](
      FollowUp("Patient_A", "any_reason", makeTS(2006, 6, 1), makeTS(2008, 11, 30)),
      FollowUp("Patient_B", "any_reason", makeTS(2006, 7, 1), makeTS(2007, 7, 1)),
      FollowUp("Patient_C", "any_reason", makeTS(2006, 8, 1), makeTS(2009, 12, 31))
    ).toDS()

    val expected: Dataset[Event[Exposure]] = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "PIOGLITAZONE", 1, makeTS(2008, 1, 1), Some(makeTS(2008, 11, 30))),
      Exposure("Patient_B", "NA", "BENFLUOREX", 1, makeTS(2007, 1, 1), Some(makeTS(2007, 7, 1)))
    ).toDS()
    val exposureAdder = UnlimitedExposureAdder(0.months, 1, 0.months)

    val result = exposureAdder.toExposure(followUp)(input)
    assertDSs(result, expected)
  }
}
