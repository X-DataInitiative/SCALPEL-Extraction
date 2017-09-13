package fr.polytechnique.cmap.cnam.etl.old_root.exposures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.old_root.{FilteringConfig, FlatEvent}
import fr.polytechnique.cmap.cnam.util.functions._

class ExposuresTransformerSuite extends SharedContext {

  override def beforeEach(): Unit = {
    super.beforeEach()
    val c = FilteringConfig.getClass.getDeclaredConstructor()
    c.setAccessible(true)
    c.newInstance()
  }

  "transform" should "return a valid Dataset for a known input" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "followUpPeriod",
        "death", 900.0, makeTS(2007, 1, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 10, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 4, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 7, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2006, 7, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 1, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 5, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 8, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2007, 11, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 6, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "SULFONYLUREA", 1.0, makeTS(2008, 8, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2006, 8, 1), Some(makeTS(2008, 9, 1)))
    ).toDF

    // When
    val result = ExposuresTransformer(ExposuresConfig.init()).transform(input)

    // Then
    assertDFs(result.toDF, expected)
 }

  it should "return a valid Dataset for a known input when filterDelayedPatients is false" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExposuresConfig.init().copy(filterDelayedPatients = false)
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "followUpPeriod",
        "death", 900.0, makeTS(2007, 1, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 10, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 4, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 7, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2006, 7, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 1, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 5, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 8, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2007, 11, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 6, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "SULFONYLUREA", 1.0, makeTS(2008, 8, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2006, 8, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 11, 1), Some(makeTS(2008, 9, 1)))
    ).toDF

    // When
    val result = ExposuresTransformer(config).transform(input)

    // Then
    assertDFs(result.toDF, expected)
 }

  it should "also return a valid Dataset when cumulativeExposureType is purchase-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExposuresConfig.init().copy(
      minPurchases = 2,
      purchasesWindow = 6,
      startDelay = 3,
      filterDelayedPatients = false,
      weightAggStrategy = WeightAggStrategy.PurchaseBased
    )
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "followUpPeriod",
        "death", 900.0, makeTS(2007, 1, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 1, 31), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 15), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2008, 10, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 4, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 5, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "molecule",
        "SULFONYLUREA", 900.0, makeTS(2008, 5, 10), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2006, 7, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 3, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 3, 15), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 3, 30), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2007, 11, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 6, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 6, 30), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 1, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 2.0, makeTS(2007, 5, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 3.0, makeTS(2008, 10, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "SULFONYLUREA", 1.0, makeTS(2008, 4, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "SULFONYLUREA", 2.0, makeTS(2008, 5, 10), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2006, 3, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 2.0, makeTS(2006, 3, 30), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 2.0, makeTS(2007, 6, 30), Some(makeTS(2008, 9, 1)))
    ).toDF

    // When
    val result = ExposuresTransformer(config).transform(input).toDF

    // Then
    assertDFs(result, expected)
  }

  it should "also return a valid Dataset when cumulativeExposureType is time-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExposuresConfig.init().copy(
      minPurchases = 2,
      purchasesWindow = 6,
      startDelay = 3,
      filterDelayedPatients = false,
      periodStrategy = ExposurePeriodStrategy.Limited,
      weightAggStrategy = WeightAggStrategy.TimeBased
    )
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 12, 31)), "followUpPeriod",
        "death", 1.0, makeTS(2006,  1, 1), Some(makeTS(2009, 1, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2006,  1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2006,  2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2006,  5, 1), None),
      // Patient_A exposure 1
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2007,  1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2007,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2007,  8, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2007,  9, 1), None),
      // Patient_A exposure 2
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  3, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  4, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  6, 1), None),
      // Patient_A incomplete exposure
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008, 12, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2009, 11, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2009, 12, 1), None),
      // Patient_B exposure 1
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "followUpPeriod",
        "trackloss", 1.0, makeTS(2008, 1, 1), Some(makeTS(2008, 4, 30))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  1, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  3, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  4, 1), None),
      // Patient_C exposure 1
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "followUpPeriod",
        "disease", 1.0, makeTS(2008,  1, 1), Some(makeTS(2009, 5, 1))),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "disease",
        "C67", 1.0, makeTS(2009, 5, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  1, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  3, 1), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "exposure",
        "PIOGLITAZONE", 3.0, makeTS(2006,  2, 1), Some(makeTS(2009, 1, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "exposure",
        "PIOGLITAZONE", 5.0, makeTS(2008,  4, 1), Some(makeTS(2009, 1, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "exposure",
        "SULFONYLUREA", 3.0, makeTS(2007, 6, 1), Some(makeTS(2009, 1, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "exposure",
        "PIOGLITAZONE", 2.0, makeTS(2008,  2, 1), Some(makeTS(2008, 4, 30))),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "exposure",
        "SULFONYLUREA", 1.0, makeTS(2008,  2, 1), Some(makeTS(2009, 5, 1)))
    ).toDF

    // When
    val result = ExposuresTransformer(config).transform(input).toDF

    // Then
    assertDFs(result, expected)
  }
}


