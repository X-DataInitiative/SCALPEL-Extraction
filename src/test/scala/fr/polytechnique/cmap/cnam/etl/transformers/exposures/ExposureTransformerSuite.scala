// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event, Exposure, FollowUp}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class ExposureTransformerSuite extends SharedContext {
  "toExposure" should "transform drugs to exposure based on parameters" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val input: Dataset[Event[Drug]] = Seq(
      Drug("Patient_A", "Antidepresseurs", 2, makeTS(2014, 6, 8)),
      Drug("Patient_A", "Antidepresseurs", 2, makeTS(2014, 7, 1)),
      Drug("Patient_B", "Antidepresseurs", 2, makeTS(2014, 2, 5)),
      Drug("Patient_B", "Antidepresseurs", 2, makeTS(2014, 9, 1))
    ).toDS
    val followUp: Dataset[Event[FollowUp]] = Seq(
      FollowUp("Patient_A", "any_reason", makeTS(2014, 6, 1), makeTS(2016, 12, 31)),
      FollowUp("Patient_B", "any_reason", makeTS(2014, 7, 1), makeTS(2016, 7, 1)),
      FollowUp("Patient_C", "any_reason", makeTS(2016, 8, 1), makeTS(2017, 12, 31))
    ).toDS

    val expected: Dataset[Event[Exposure]] = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 8), Some(makeTS(2014, 8, 7))),
      Exposure("Patient_B", "NA", "Antidepresseurs", 1D, makeTS(2014, 9, 1), Some(makeTS(2014, 10, 1)))
    ).toDS
    val exposureTransformer = new ExposureTransformer(
      new ExposuresTransformerConfig(
        LimitedExposureAdder(
          0.days,
          15.days,
          90.days,
          30.days,
          PurchaseCountBased
        )
      )
    )

    val result = exposureTransformer.transform(followUp)(input)
    assertDSs(expected, result)
  }

  "controlPrevalentExposure" should "should filter out Exposures which occur entirely before FollowUp" in {
    //Given
    val input = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 8), Some(makeTS(2014, 8, 7))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2015, 9, 1), Some(makeTS(2015, 10, 1))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 5, 1), Some(makeTS(2014, 10, 1))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2013, 5, 1), Some(makeTS(2013, 10, 1)))
    )
    val followUp: Event[FollowUp] =
      FollowUp("Patient_A", "any_reason", makeTS(2014, 6, 1), makeTS(2014, 12, 31))


    val expected = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 8), Some(makeTS(2014, 8, 7))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2015, 9, 1), Some(makeTS(2015, 10, 1))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 5, 1), Some(makeTS(2014, 10, 1)))
    )

    val exposureTransformer = new ExposureTransformer(
      new ExposuresTransformerConfig(
        LimitedExposureAdder(
          0.days,
          15.days,
          90.days,
          30.days,
          PurchaseCountBased
        )
      )
    )

    val result = input.flatMap(exposureTransformer.controlPrevalentExposure(_)(followUp))
    assertResult(expected)(result)
  }

  "controlDelayedExposure" should "should filter out Exposures which occur entirely after FollowUp" in {
    //Given
    val input = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 8), Some(makeTS(2014, 8, 7))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2015, 9, 1), Some(makeTS(2015, 10, 1))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 5, 1), Some(makeTS(2014, 10, 1))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2013, 5, 1), Some(makeTS(2013, 10, 1)))
    )
    val followUp: Event[FollowUp] =
      FollowUp("Patient_A", "any_reason", makeTS(2014, 6, 1), makeTS(2014, 12, 31))


    val expected = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 8), Some(makeTS(2014, 8, 7))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 5, 1), Some(makeTS(2014, 10, 1))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2013, 5, 1), Some(makeTS(2013, 10, 1)))
    )

    val exposureTransformer = new ExposureTransformer(
      new ExposuresTransformerConfig(
        LimitedExposureAdder(
          0.days,
          15.days,
          90.days,
          30.days,
          PurchaseCountBased
        )
      )
    )

    val result = input.flatMap(exposureTransformer.controlDelayedExposure(_)(followUp))
    assertResult(expected)(result)
  }

  "regulateExposureEndWithFollowUp" should "should end Exposures as latest as FollowUp end date" in {
    //Given
    val input = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 8), Some(makeTS(2014, 8, 7))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 5, 1), Some(makeTS(2015, 10, 1)))
    )
    val followUp: Event[FollowUp] =
      FollowUp("Patient_A", "any_reason", makeTS(2014, 6, 1), makeTS(2014, 12, 31))


    val expected = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 8), Some(makeTS(2014, 8, 7))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 5, 1), Some(makeTS(2014, 12, 31)))
    )

    val exposureTransformer = new ExposureTransformer(
      new ExposuresTransformerConfig(
        LimitedExposureAdder(
          0.days,
          15.days,
          90.days,
          30.days,
          PurchaseCountBased
        )
      )
    )

    val result = input.map(exposureTransformer.regulateExposureEndWithFollowUp(_)(followUp))
    assertResult(expected)(result)
  }

  "regulateExposureStartWithFollowUp" should "should start Exposures as earliest as FollowUp start" in {
    //Given
    val input = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 8), Some(makeTS(2014, 8, 7))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 5, 1), Some(makeTS(2014, 10, 1)))
    )
    val followUp: Event[FollowUp] =
      FollowUp("Patient_A", "any_reason", makeTS(2014, 6, 1), makeTS(2014, 12, 31))


    val expected = Seq[Event[Exposure]](
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 8), Some(makeTS(2014, 8, 7))),
      Exposure("Patient_A", "NA", "Antidepresseurs", 1D, makeTS(2014, 6, 1), Some(makeTS(2014, 10, 1)))
    )

    val exposureTransformer = new ExposureTransformer(
      new ExposuresTransformerConfig(
        LimitedExposureAdder(
          0.days,
          15.days,
          90.days,
          30.days,
          PurchaseCountBased
        )
      )
    )

    val result = input.map(exposureTransformer.regulateExposureStartWithFollowUp(_)(followUp))
    assertResult(expected)(result)
  }

}

