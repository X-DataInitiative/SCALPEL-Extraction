package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Exposure, Molecule}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
import fr.polytechnique.cmap.cnam.util.functions._

class ExposuresTransformerSuite extends SharedContext {

  "transform" should "return a valid Dataset for a known input" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val patients = Seq(
      (Patient("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11))), FollowUp("Patient_A", makeTS(2007, 1, 1), makeTS(2009, 7, 11), "death")),
      (Patient("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1))), FollowUp("Patient_B", makeTS(2006, 7, 1), makeTS(2008, 9, 1), "death")),
      (Patient("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1))), FollowUp("Patient_B.1", makeTS(2007, 11, 1), makeTS(2008, 9, 1), "death"))
    ).toDS

    val prescriptions = Seq(
      //Event("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "followUpPeriod",
      //  "death", 900.0, makeTS(2007, 1, 1), Some(makeTS(2009, 7, 11))),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 1, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 2, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 10, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 900.0, makeTS(2008, 4, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 900.0, makeTS(2008, 5, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 900.0, makeTS(2008, 7, 1)),
      //Event("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
      //  "trackloss", 900.0, makeTS(2006, 7, 1), Some(makeTS(2008, 9, 1))),
      Molecule("Patient_B", "PIOGLITAZONE", 900.0, makeTS(2006, 1, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 900.0, makeTS(2006, 5, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 900.0, makeTS(2006, 8, 1)),
      //Event("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
      //  "trackloss", 900.0, makeTS(2007, 11, 1), Some(makeTS(2008, 9, 1))),
      Molecule("Patient_B.1", "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1)),
      Molecule("Patient_B.1", "PIOGLITAZONE", 900.0, makeTS(2007, 6, 1)),
      Molecule("Patient_B.1", "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1))
    ).toDS

    val expected = Seq(
      Exposure("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), makeTS(2009, 7, 11)),
      Exposure("Patient_A", "SULFONYLUREA", 1.0, makeTS(2008, 8, 1), makeTS(2009, 7, 11)),
      Exposure("Patient_B", "PIOGLITAZONE", 1.0, makeTS(2006, 8, 1), makeTS(2008, 9, 1))
    ).toDS

    val exposure = ExposureDefinition(
      studyStart =  makeTS(2006, 1, 1),
      diseaseCode = "C67",
      filterDelayedPatients = true)

    // When
    val result = new ExposuresTransformer(exposure).transform(patients, prescriptions)

    // Then
    assertDSs(result, expected, true)
 }

  it should "return a valid Dataset for a known input when filterDelayedPatients is false" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val exposure = ExposureDefinition(
      studyStart =  makeTS(2006, 1, 1),
      diseaseCode = "C67",
      filterDelayedPatients = false)

    val patients = Seq(
      (Patient("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11))), FollowUp("Patient_A", makeTS(2007, 1, 1), makeTS(2009, 7, 11), "death")),
      (Patient("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1))), FollowUp("Patient_B", makeTS(2006, 7, 1), makeTS(2008, 9, 1), "death")),
      (Patient("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1))), FollowUp("Patient_B.1", makeTS(2007, 11, 1), makeTS(2008, 9, 1), "death"))
    ).toDS

    val molecules = Seq(
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 1, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 2, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 10, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 900.0, makeTS(2008, 4, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 900.0, makeTS(2008, 5, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 900.0, makeTS(2008, 7, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 900.0, makeTS(2006, 1, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 900.0, makeTS(2006, 5, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 900.0, makeTS(2006, 8, 1)),
      Molecule("Patient_B.1", "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1)),
      Molecule("Patient_B.1", "PIOGLITAZONE", 900.0, makeTS(2007, 6, 1)),
      Molecule("Patient_B.1", "PIOGLITAZONE", 900.0, makeTS(2007, 8, 1))
    ).toDS

    val expected = Seq(
      Exposure("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), makeTS(2009, 7, 11)),
      Exposure("Patient_A", "SULFONYLUREA", 1.0, makeTS(2008, 8, 1), makeTS(2009, 7, 11)),
      Exposure("Patient_B", "PIOGLITAZONE", 1.0, makeTS(2006, 8, 1), makeTS(2008, 9, 1)),
      Exposure("Patient_B.1", "PIOGLITAZONE", 1.0, makeTS(2007, 11, 1), makeTS(2008, 9, 1))
    ).toDS

    // When
    val result = new ExposuresTransformer(exposure).transform(patients, molecules)

    // Then
    assertDSs(result, expected)
 }

  it should "also return a valid Dataset when cumulativeExposureType is purchase-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val exposure = ExposureDefinition(
      studyStart =  makeTS(2006, 1, 1),
      diseaseCode = "C67",
      filterDelayedPatients = false,
      weightAggStrategy = WeightAggStrategy.PurchaseBased
    )

    val patients = Seq(
      (Patient("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11))), FollowUp("Patient_A", makeTS(2007, 1, 1), makeTS(2009, 7, 11), "death")),
      (Patient("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1))), FollowUp("Patient_B", makeTS(2006, 7, 1), makeTS(2008, 9, 1), "death")),
      (Patient("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1))), FollowUp("Patient_B.1", makeTS(2007, 11, 1), makeTS(2008, 9, 1), "death"))
    ).toDS

    val molecules = Seq(
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 1, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 1, 31)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2007, 5, 15)),
      Molecule("Patient_A", "PIOGLITAZONE", 900.0, makeTS(2008, 10, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 900.0, makeTS(2008, 4, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 900.0, makeTS(2008, 5, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 900.0, makeTS(2008, 5, 10)),
      Molecule("Patient_B", "PIOGLITAZONE", 900.0, makeTS(2006, 3, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 900.0, makeTS(2006, 3, 15)),
      Molecule("Patient_B", "PIOGLITAZONE", 900.0, makeTS(2006, 3, 30)),
      Molecule("Patient_B.1", "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1)),
      Molecule("Patient_B.1", "PIOGLITAZONE", 900.0, makeTS(2007, 6, 1)),
      Molecule("Patient_B.1", "PIOGLITAZONE", 900.0, makeTS(2007, 6, 30))
    ).toDS

    val expected = Seq(
      Exposure("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2007, 1, 1), makeTS(2009, 7, 11)),
      Exposure("Patient_A", "PIOGLITAZONE", 2.0, makeTS(2007, 5, 1), makeTS(2009, 7, 11)),
      Exposure("Patient_A", "PIOGLITAZONE", 3.0, makeTS(2008, 10, 1), makeTS(2009, 7, 11)),
      Exposure("Patient_A", "SULFONYLUREA", 1.0, makeTS(2008, 4, 1), makeTS(2009, 7, 11)),
      Exposure("Patient_A", "SULFONYLUREA", 2.0, makeTS(2008, 5, 10), makeTS(2009, 7, 11)),
      Exposure("Patient_B", "PIOGLITAZONE", 1.0, makeTS(2006, 3, 1), makeTS(2008, 9, 1)),
      Exposure("Patient_B", "PIOGLITAZONE", 2.0, makeTS(2006, 3, 30), makeTS(2008, 9, 1)),
      Exposure("Patient_B.1", "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), makeTS(2008, 9, 1)),
      Exposure("Patient_B.1", "PIOGLITAZONE", 2.0, makeTS(2007, 6, 30), makeTS(2008, 9, 1))
    ).toDS

    // When
    val result = new ExposuresTransformer(exposure).transform(patients, molecules)

    // Then
    assertDSs(result, expected)
  }

  it should "also return a valid Dataset when cumulativeExposureType is time-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val exposure = ExposureDefinition(
      studyStart =  makeTS(2006, 1, 1),
      diseaseCode = "C67",
      filterDelayedPatients = false,
      periodStrategy = ExposurePeriodStrategy.Limited,
      weightAggStrategy = WeightAggStrategy.TimeBased
    )
    val patients = Seq(
      (Patient("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 12, 31))), FollowUp("Patient_A", makeTS(2006,  1, 1), makeTS(2009, 1, 1), "death")),
      (Patient("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31))), FollowUp("Patient_B", makeTS(2008, 1, 1), makeTS(2008, 4, 30), "trackloss")),
      (Patient("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31))), FollowUp("Patient_C", makeTS(2008,  1, 1), makeTS(2009, 5, 1), "trackloss"))
    ).toDS
    val molecules = Seq(
      Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2006,  1, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2006,  2, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2006,  5, 1)),
      // Patient_A exposure 1
      Molecule("Patient_A", "SULFONYLUREA", 1.0, makeTS(2007,  1, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 1.0, makeTS(2007,  6, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 1.0, makeTS(2007,  8, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 1.0, makeTS(2007,  9, 1)),
      // Patient_A exposure 2
      Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2008,  3, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2008,  4, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2008,  6, 1)),
      // Patient_A incomplete exposure
      Molecule("Patient_A", "SULFONYLUREA", 1.0, makeTS(2008,  6, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 1.0, makeTS(2008, 12, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 1.0, makeTS(2009, 11, 1)),
      Molecule("Patient_A", "SULFONYLUREA", 1.0, makeTS(2009, 12, 1)),
      // Patient_B exposure 1
      Molecule("Patient_B", "PIOGLITAZONE", 1.0, makeTS(2008,  1, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 1.0, makeTS(2008,  3, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 1.0, makeTS(2008,  4, 1)),
      // Patient_C exposure 1
      //FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "disease",
      //  "C67", 1.0, makeTS(2009, 5, 1), None),
      Molecule("Patient_C", "SULFONYLUREA", 1.0, makeTS(2008,  1, 1)),
      Molecule("Patient_C", "SULFONYLUREA", 1.0, makeTS(2008,  2, 1)),
      Molecule("Patient_C", "SULFONYLUREA", 1.0, makeTS(2008,  3, 1))
    ).toDS

    val expected = Seq(
      Exposure("Patient_A", "PIOGLITAZONE", 3.0, makeTS(2006,  2, 1), makeTS(2009, 1, 1)),
      Exposure("Patient_A", "PIOGLITAZONE", 5.0, makeTS(2008,  4, 1), makeTS(2009, 1, 1)),
      Exposure("Patient_A", "SULFONYLUREA", 3.0, makeTS(2007, 6, 1), makeTS(2009, 1, 1)),
      Exposure("Patient_B", "PIOGLITAZONE", 2.0, makeTS(2008,  2, 1), makeTS(2008, 4, 30)),
      Exposure("Patient_C", "SULFONYLUREA", 1.0, makeTS(2008,  2, 1), makeTS(2009, 5, 1))
    ).toDS

    // When
    val result = new ExposuresTransformer(exposure).transform(patients, molecules)

    // Then
    assertDSs(result, expected)
  }

  "constructor" should "create correct Transformer" in {
    // Given
    val config = ExposureDefinition(
      periodStrategy = ExposurePeriodStrategy.Limited,
      minPurchases = 2,
      purchasesWindow = 3,
      startDelay = 4,
      weightAggStrategy = WeightAggStrategy.NonCumulative,
      cumulativeExposureWindow = 5,
      cumulativeStartThreshold = 6,
      cumulativeEndThreshold = 7,
      dosageLevelIntervals = List(8),
      purchaseIntervals = List(9, 10),
        studyStart = makeTS(2006, 1, 1),
      filterDelayedPatients = true,
      diseaseCode = "FootCancer"
    )

    // When
    val result = new ExposuresTransformer(config)

    // Then
    assert(result.studyStart == makeTS(2006, 1, 1))
    assert(result.diseaseCode == "FootCancer")
    assert(result.exposurePeriodStrategy == ExposurePeriodStrategy.Limited)
    assert(result.minPurchases == 2)
    assert(result.purchasesWindow ==  3)
    assert(result.startDelay == 4)
    assert(result.weightAggStrategy == WeightAggStrategy.NonCumulative)
    assert(result.filterDelayedPatients)
    assert(result.cumulativeExposureWindow == 5)
    assert(result.cumulativeStartThreshold == 6)
    assert(result.cumulativeEndThreshold == 7)
    assert(result.dosageLevelIntervals == List(8))
    assert(result.purchaseIntervals == List(9, 10))


  }
}


