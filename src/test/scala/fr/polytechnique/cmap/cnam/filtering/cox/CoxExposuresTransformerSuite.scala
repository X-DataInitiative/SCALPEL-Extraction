package fr.polytechnique.cmap.cnam.filtering.cox

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.FlatEvent
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class CoxExposuresTransformerSuite extends SharedContext {

  "filterPatients" should "always drop patients who got the disease before the follow up start" +
    "and patients exposed after first year of study if includesDelayedPatients variable is false" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "m1", makeTS(2006, 1, 20), makeTS(2006, 6, 29)),
      ("Patient_A", "molecule", "m2", makeTS(2006, 1, 1), makeTS(2006, 6, 29)),
      ("Patient_A", "molecule", "m3", makeTS(2006, 1, 10), makeTS(2006, 6, 29)),
      ("Patient_A", "disease", "C67", makeTS(2006, 1, 1), makeTS(2006, 6, 29)),
      ("Patient_B", "molecule", "m1", makeTS(2009, 1, 1), makeTS(2009, 6, 30)),
      ("Patient_B", "molecule", "m2", makeTS(2009, 1, 1), makeTS(2009, 6, 30)),
      ("Patient_B", "disease", "m2", makeTS(2009, 1, 1), makeTS(2009, 6, 30)),
      ("Patient_C", "molecule", "m1", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "molecule", "m2", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "disease", "C67", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF("patientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_C", "molecule", "m1"),
      ("Patient_C", "molecule", "m2"),
      ("Patient_C", "disease", "C67")
    ).toDF("patientID", "category", "eventId")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val result = input.filterPatients(filterDelayedPatients = true)
      .select("patientID", "category", "eventId")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  it should "include patients who exposed after first year of study if the includesDelayedPatients " +
    "variable is set to true" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "m1", makeTS(2006, 1, 20), makeTS(2006, 6, 29)),
      ("Patient_A", "molecule", "m2", makeTS(2006, 1, 1), makeTS(2006, 6, 29)),
      ("Patient_A", "molecule", "m3", makeTS(2006, 1, 10), makeTS(2006, 6, 29)),
      ("Patient_A", "disease", "C67", makeTS(2005, 1, 1), makeTS(2006, 6, 29)),
      ("Patient_B", "molecule", "m1", makeTS(2009, 1, 20), makeTS(2009, 6, 29)),
      ("Patient_B", "molecule", "m2", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
      ("Patient_B", "disease", "C67", makeTS(2009, 9, 10), makeTS(2009, 6, 29)),
      ("Patient_B.1", "molecule", "m1", makeTS(2009, 1, 20), makeTS(2009, 6, 29)),
      ("Patient_B.1", "molecule", "m2", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
      ("Patient_B.1", "disease", "C67", makeTS(2009, 5, 10), makeTS(2009, 6, 29)),
      ("Patient_C", "molecule", "m1", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "molecule", "m2", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "disease", "C67", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF("patientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_B", "molecule", "m1"),
      ("Patient_B", "molecule", "m2"),
      ("Patient_B", "disease", "C67"),
      ("Patient_C", "molecule", "m1"),
      ("Patient_C", "molecule", "m2"),
      ("Patient_C", "disease", "C67")
    ).toDF("patientID", "category", "eventId")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val result = input.filterPatients(filterDelayedPatients = false)
      .select("patientID", "category", "eventId")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  "withPeriodicExposureStart" should "add exposureStart column correctly if the minPurchases value " +
    "is passed as 2" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
    val coxExposureDefintion = CoxExposureDefinition(
      minPurchases = 2,
      purchasesWindow = 6,
      startDelay = 3
    )
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 3, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 4, 1), makeTS(2008, 6, 29)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 12, 1))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 7, 1))),
      ("Patient_B", "PIOGLITAZONE", None),
      ("Patient_B", "BENFLUOREX", None)
    ).toDF("PatientID", "eventId", "exposureStart")


    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val result = input.withExposureStart(coxExposureDefintion).select("PatientID", "eventId", "exposureStart")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  it should "compute the exposureStart date correctly if the minPurchases value is passed as 1" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
    val coxExposureDefintion = CoxExposureDefinition(
      minPurchases = 1,
      purchasesWindow = 6,
      startDelay = 3
    )
    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 3, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 3, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2009, 4, 1), makeTS(2008, 6, 29)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2009, 6, 1), makeTS(2009, 6, 29))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29))),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 6, 1))),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2009, 6, 1))),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2009, 6, 29))),
      ("Patient_B", "BENFLUOREX", Some(makeTS(2009, 9, 1)))
    ).toDF("PatientID", "eventId", "exposureStart")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val result = input.withExposureStart(coxExposureDefintion)
      .select("PatientID", "eventId", "exposureStart")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
  }

  "withExposureStart" should "compute exposureStart and weight correctly if the cumulativeExposureType " +
    "is purchase-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
    val coxExposureDefinition = CoxExposureDefinition(
      minPurchases = 2,
      purchasesWindow = 6,
      startDelay = 3,
      cumulativeExposureType = CoxConfig.ExposureType.PurchaseBasedCumulative
    )

    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 10), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 10), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 7, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 9, 1), makeTS(2008, 6, 29)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 1, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 8, 1)), Some(2.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 9, 1)), Some(4.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 9, 1)), Some(4.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 10, 1)), Some(5.0)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 7, 1)), Some(1.0)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 9, 1)), Some(2.0)),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2009, 1, 1)), Some(1.0)),
      ("Patient_B", "BENFLUOREX", Some(makeTS(2007, 1, 1)), Some(1.0))
    ).toDF("PatientID", "eventId", "exposureStart", "weight")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val result = input.withExposureStart(coxExposureDefinition)
      .select("PatientID", "eventId", "exposureStart", "weight")
      .orderBy("PatientID", "eventId", "exposureStart")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    result.printSchema
    println("Expected:")
    expected.show
    expected.printSchema
    assert(result === expected)
  }

  it should "compute exposureStart and weight correctly for the purchase-based cumulativeExposure" +
    "when the window is greater than 1 is " in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
    val coxExposureDefintion = CoxExposureDefinition(
      minPurchases = 2,
      purchasesWindow = 6,
      startDelay = 3,
      cumulativeExposureType = CoxConfig.ExposureType.PurchaseBasedCumulative,
      cumulativeExposureWindow = 3
    )

    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 9, 10), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 10, 10), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 7, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "SULFONYLUREA", makeTS(2008, 9, 1), makeTS(2008, 6, 29)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 6, 29)),
      ("Patient_B", "molecule", "BENFLUOREX", makeTS(2007, 1, 1), makeTS(2009, 6, 29))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 1, 1)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 7, 1)), Some(4.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 7, 1)), Some(4.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 7, 1)), Some(4.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 10, 1)), Some(5.0)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 7, 1)), Some(2.0)),
      ("Patient_A", "SULFONYLUREA", Some(makeTS(2008, 7, 1)), Some(2.0)),
      ("Patient_B", "PIOGLITAZONE", Some(makeTS(2009, 1, 1)), Some(1.0)),
      ("Patient_B", "BENFLUOREX", Some(makeTS(2007, 1, 1)), Some(1.0))
    ).toDF("PatientID", "eventId", "exposureStart", "weight")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val result = input.withExposureStart(coxExposureDefintion)
      .select("PatientID", "eventId", "exposureStart", "weight")
      .orderBy("PatientID", "eventId", "exposureStart")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    result.printSchema
    println("Expected:")
    expected.show
    expected.printSchema
    assert(result === expected)
  }

  it should "compute exposureStart and weight correctly if the cumulativeExposureType " +
    "is time-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
    val coxExposureDefintion = CoxExposureDefinition(
      minPurchases = 1,
      purchasesWindow = 6,
      startDelay = 3,
      cumulativeExposureType = CoxConfig.ExposureType.TimeBasedCumulative
    )
    val input = Seq(
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  1, 1), None:Option[Timestamp], makeTS(1001,  1, 1)),
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None, makeTS(1001,  1, 1)),
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  5, 1), None, makeTS(1001,  1, 1)),
      // Patient_A exposure 2
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2009,  6, 1), None, makeTS(1001,  1, 1)),
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2009,  9, 1), None, makeTS(1001,  1, 1)),
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2010,  1, 1), None, makeTS(1001,  1, 1)),
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2010,  12, 1), None, makeTS(1001,  1, 1)),
      // Patient_A exposure 3
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  6, 1), None, makeTS(1001,  1, 1)),
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008, 7, 1), None, makeTS(1001,  1, 1)),
      ("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2011, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008, 10, 1), None, makeTS(1001,  1, 1)),
      // Patient_B
      ("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  1, 1), None, makeTS(2001,  1, 1)),
      ("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None, makeTS(2001,  1, 1)),
      ("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  4, 1), None, makeTS(2001,  1, 1))
    ).toDF("patientID", "gender", "birthDate", "deathDate", "category", "eventId",
      "weight", "start", "end", "followUpEnd")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", 3.0, makeTS(2008, 2, 1), makeTS(2008, 5, 1), makeTS(1001, 1, 1)),
      ("Patient_A", "PIOGLITAZONE", 7.0, makeTS(2009, 9, 1), makeTS(2010, 1, 1), makeTS(1001, 1, 1)),
      ("Patient_A", "SULFONYLUREA", 3.0, makeTS(2008, 7, 1), makeTS(2008, 10, 1), makeTS(1001, 1, 1)),
      ("Patient_B", "PIOGLITAZONE", 2.0, makeTS(2008, 2, 1), makeTS(2008, 4, 1), makeTS(2001, 1, 1))
    ).toDF("patientID", "eventId", "weight", "start", "end", "followUpEnd")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val exposure = input.withTimeBasedCumulativeExposure(4, 6)
    val result = exposure.select("patientID", "eventId", "weight", "start", "end", "followUpEnd")

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result.toDF === expected)

  }

  it should "compute exposureStart and weight for periodicExposure definition " +
    "if the cumulativeExposureType variable is not set or don't match any case " in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
    val coxExposureDefintion = CoxExposureDefinition(
      minPurchases = 1,
      purchasesWindow = 6,
      startDelay = 3
    )

    val input = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 3, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 6, 29)),
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), makeTS(2008, 6, 29))
    ).toDF("PatientID", "category", "eventId", "start", "followUpStart")

    val expected = Seq(
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29)), Some(1.0)),
      ("Patient_A", "PIOGLITAZONE", Some(makeTS(2008, 6, 29)), Some(1.0))
    ).toDF("PatientID", "eventId", "exposureStart", "weight")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxExposuresTransformer.ExposuresDataFrame
    val result = input.withExposureStart(coxExposureDefintion)
      .select("PatientID", "eventId", "exposureStart", "weight")

    // Then
    import RichDataFrames._
    println("Result:")
    result.show
    println("Expected:")
    expected.show
    assert(result === expected)
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
    val result = CoxExposuresTransformer.transform(input)

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result.toDF === expected)
  }

  it should "return a valid Dataset for a known input when filterDelayedPatients is false" in {

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
        "PIOGLITAZONE", 1.0, makeTS(2006, 8, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 11, 1), Some(makeTS(2008, 9, 1)))
    ).toDF

    // When
    val result = CoxExposuresTransformer.transform(
      input, filterDelayedPatients = false, CoxConfig.exposureDefinition)

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result.toDF === expected)
  }

  it should "also return a valid Dataset when cumulativeExposureType is purchase-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
    val coxExposureDefintion = CoxExposureDefinition(
      minPurchases = 2,
      purchasesWindow = 6,
      startDelay = 3,
      cumulativeExposureType = CoxConfig.ExposureType.PurchaseBasedCumulative
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
        "PIOGLITAZONE", 2.0, makeTS(2007, 1, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 4.0, makeTS(2007, 5, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 5.0, makeTS(2008, 10, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "SULFONYLUREA", 1.0, makeTS(2008, 4, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "SULFONYLUREA", 3.0, makeTS(2008, 5, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 3.0, makeTS(2006, 3, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 3.0, makeTS(2007, 6, 1), Some(makeTS(2008, 9, 1)))
    ).toDF

    // When
    val coxExposure = CoxExposuresTransformer.transform(
      input, filterDelayedPatients = false, coxExposureDefintion)
    val result = coxExposure.toDF.orderBy("patientID", "eventId", "start")

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result === expected)
  }

  it should "also return a valid Dataset when cumulativeExposureType is purchase-based and " +
    "purchase-window is greater than one" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
    val coxExposureDefintion = CoxExposureDefinition(
      minPurchases = 2,
      purchasesWindow = 6,
      startDelay = 3,
      cumulativeExposureType = CoxConfig.ExposureType.PurchaseBasedCumulative,
      cumulativeExposureWindow = 3
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
        "SULFONYLUREA", 900.0, makeTS(2008, 6, 10), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2006, 7, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 3, 1), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 3, 15), None),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2006, 2, 30), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2007, 11, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 5, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 6, 1), None),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "molecule",
        "PIOGLITAZONE", 900.0, makeTS(2007, 7, 30), None)
    ).toDS

    val expected = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 2.0, makeTS(2007, 1, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 4.0, makeTS(2007, 4, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 5.0, makeTS(2008, 10, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "SULFONYLUREA", 3.0, makeTS(2008, 4, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 3.0, makeTS(2006, 1, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 2.0, makeTS(2007, 4, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_B.1", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 3.0, makeTS(2007, 7, 1), Some(makeTS(2008, 9, 1)))
    ).toDF

    // When
    val coxExposure = CoxExposuresTransformer.transform(
      input, filterDelayedPatients = false, coxExposureDefintion)
    val result = coxExposure.toDF.orderBy("patientID", "eventId", "start")

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result === expected)
  }

  it should "also return a valid Dataset when cumulativeExposureType is time-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
    val coxExposureDefintion = CoxExposureDefinition(
      minPurchases = 2,
      purchasesWindow = 6,
      startDelay = 3,
      cumulativeExposureType = CoxConfig.ExposureType.TimeBasedCumulative
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
    val coxExposures = CoxExposuresTransformer.transform(input, filterDelayedPatients = false,
      coxExposureDefintion)
    val result = coxExposures.toDF.orderBy("patientID", "eventId", "start")

    // Then
    result.show
    expected.show
    import RichDataFrames._
    assert(result.toDF === expected)
  }
}


