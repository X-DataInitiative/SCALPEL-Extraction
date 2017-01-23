package fr.polytechnique.cmap.cnam.filtering.cox

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.{FilteringConfig, FlatEvent}
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS

/**
  * Created by sathiya on 24/11/16.
  */
class CoxMainSuite extends SharedContext {

  override def beforeEach(): Unit = {
    super.beforeEach()
    val filteringConfig = FilteringConfig.getClass.getDeclaredConstructor()
    val coxConfig = CoxConfig.getClass.getDeclaredConstructor()
    filteringConfig.setAccessible(true)
    filteringConfig.newInstance()
    coxConfig.setAccessible(true)
    coxConfig.newInstance()
  }

  "run" should "correctly run the full pipeline from Filtering till CoxFeaturing" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/filtering-broad.conf"
    val expectedResult = Seq[CoxFeature]().toDS.toDF

    // When
    val coxFeatures = CoxMain.run(sqlContext, Map("conf" -> configPath))
    val result = coxFeatures.get.toDF.orderBy("patientID")

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  //TODO: We should update the dummy files and test only the above test case in the future.
  // We are testing it for the moment because the dummy data don't have any meaningful exposure.
  "coxFeaturing" should "correctly run the CoxFeaturing for the given FlatEvents" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/filtering-broad.conf"
    val flatEvents: Dataset[FlatEvent] = Seq(
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "PIOGLITAZONE", 1.0, makeTS(2006, 1, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "PIOGLITAZONE", 1.0, makeTS(2006, 8, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "PIOGLITAZONE", 1.0, makeTS(2006, 10, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "PIOGLITAZONE", 1.0, makeTS(2006, 12, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "ROSIGLITAZONE", 1.0, makeTS(2006, 2, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "ROSIGLITAZONE", 1.0, makeTS(2006, 9, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "ROSIGLITAZONE", 1.0, makeTS(2006, 10, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "ROSIGLITAZONE", 1.0, makeTS(2007, 1, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "disease", "C67", 1.0, makeTS(2007, 3, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "disease", "targetDisease", 1.0, makeTS(2007, 3, 10), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(2010, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 1, 20), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 8, 1), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 10, 1), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "C67", 1.0, makeTS(2009, 12, 10), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "targetDisease", 1.0, makeTS(2009, 12, 10), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(2010, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 1, 20), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 8, 1), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 10, 1), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "C67", 1.0, makeTS(2009, 5, 10), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "targetDisease", 1.0, makeTS(2009, 5, 10), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 2, 1), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 8, 1), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 11, 1), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "disease", "C67", 1.0, makeTS(2007, 1, 1), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "disease", "targetDisease", 1.0, makeTS(2007, 1, 1), null),
      FlatEvent("Patient_C.1", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 2, 1), null),
      FlatEvent("Patient_C.1", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 8, 1), null),
      FlatEvent("Patient_C.1", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 11, 1), null),
      FlatEvent("Patient_C.1", 1, makeTS(1979, 12, 3), null, "disease", "C67", 1.0, makeTS(2007, 1, 1), null)
    ).toDF("patientID", "gender", "birthDate", "deathDate", "category", "eventId", "weight", "start", "end")
     .as[FlatEvent]

    val expectedResult = Seq(
      CoxFeature("Patient_A", 1, 566, "45-49", 6, 8, 1, 0, 0, 0, 1, 1, 0),
      CoxFeature("Patient_C", 1, 324, "25-29", 3, 5, 1, 0, 0, 1, 0, 0, 0),
      CoxFeature("Patient_C.1", 1, 324, "25-29", 3, 40, 0, 0, 0, 1, 0, 0, 0)
    ).toDF

    // When
    val coxFeatures = CoxMain.coxFeaturing(flatEvents, Map("conf" -> configPath))
    val result = coxFeatures.get.toDF.orderBy("patientID")

    // Then
    result.show
    expectedResult.show
    import RichDataFrames._
    assert(result === expectedResult)
  }

  it should "also return a valid Dataset when cumulativeExposureType is purchase-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/cox-cumulative-purchase.conf"
    val flatEvents: Dataset[FlatEvent] = Seq(
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "PIOGLITAZONE", 1.0, makeTS(2006, 1, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "PIOGLITAZONE", 1.0, makeTS(2006, 8, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "PIOGLITAZONE", 1.0, makeTS(2006, 10, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "PIOGLITAZONE", 1.0, makeTS(2006, 12, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "ROSIGLITAZONE", 1.0, makeTS(2006, 2, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "ROSIGLITAZONE", 1.0, makeTS(2006, 9, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "ROSIGLITAZONE", 1.0, makeTS(2006, 10, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "molecule", "ROSIGLITAZONE", 1.0, makeTS(2007, 1, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "disease", "C67", 1.0, makeTS(2007, 3, 10), null),
      FlatEvent("Patient_A", 1, makeTS(1959, 10, 1), null, "disease", "targetDisease", 1.0, makeTS(2007, 3, 10), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(2010, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 1, 20), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 8, 1), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 10, 1), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "C67", 1.0, makeTS(2009, 12, 10), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "targetDisease", 1.0, makeTS(2009, 12, 10), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(2010, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 1, 20), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 8, 1), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "INSULINE", 1.0, makeTS(2009, 10, 1), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "C67", 1.0, makeTS(2009, 5, 10), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "targetDisease", 1.0, makeTS(2009, 5, 10), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 2, 1), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 8, 1), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "molecule", "INSULINE", 1.0, makeTS(2006, 8, 30), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 11, 1), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "disease", "C67", 1.0, makeTS(2007, 1, 1), null),
      FlatEvent("Patient_C", 1, makeTS(1979, 12, 3), null, "disease", "targetDisease", 1.0, makeTS(2007, 1, 1), null),
      FlatEvent("Patient_C.1", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 2, 1), null),
      FlatEvent("Patient_C.1", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 8, 1), null),
      FlatEvent("Patient_C.1", 1, makeTS(1979, 12, 3), null, "molecule", "INSULINE", 1.0, makeTS(2006, 8, 30), null),
      FlatEvent("Patient_C.1", 1, makeTS(1979, 12, 3), null, "molecule", "METFORMINE", 1.0, makeTS(2006, 11, 1), null),
      FlatEvent("Patient_C.1", 1, makeTS(1979, 12, 3), null, "disease", "C67", 1.0, makeTS(2007, 1, 1), null)
    ).toDF("patientID", "gender", "birthDate", "deathDate", "category", "eventId", "weight", "start", "end")
      .as[FlatEvent]

    val expectedResult = Seq(
      CoxFeature("Patient_A", 1, 566, "45-49", 0, 3, 0, 0, 0, 0, 1, 1, 0),
      CoxFeature("Patient_A", 1, 566, "45-49", 3, 8, 1, 0, 0, 0, 2, 2, 0),
      CoxFeature("Patient_C", 1, 324, "25-29", 0, 3, 0, 1, 0, 1, 0, 0, 0),
      CoxFeature("Patient_C", 1, 324, "25-29", 3, 5, 1, 1, 0, 2, 0, 0, 0),
      CoxFeature("Patient_C.1", 1, 324, "25-29", 0, 3, 0, 1, 0, 1, 0, 0, 0),
      CoxFeature("Patient_C.1", 1, 324, "25-29", 3, 40, 0, 1, 0, 2, 0, 0, 0)
    ).toDF

    // When
    val coxFeatures = CoxMain.coxFeaturing(flatEvents, Map("conf" -> configPath))
    val result = coxFeatures.get.toDF.orderBy("patientID", "start")

    // Then
    result.show
    expectedResult.show
    import RichDataFrames._
    assert(result === expectedResult)
  }

  it should "also return a valid result for the known input when cumulativeExposureType " +
    "is time-based" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/cox-cumulative-time.conf"
    val flatEvents: Dataset[FlatEvent] = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 12, 31)), "followUpPeriod",
        "death", 1.0, makeTS(2006,  1, 1), Some(makeTS(2009, 5, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2006,  1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2006,  2, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2006,  5, 1), None),
      // Patient_A exposure 2
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2007,  1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2007,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2007,  8, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2007,  9, 1), None),
      // Patient_A exposure 3
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  3, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  4, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  6, 1), None),
      // Patient_A exposure 4
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  6, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008, 12, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2009, 1, 1), None),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 5, 1)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2009, 2, 1), None),
      // Patient_B exposure 1
      FlatEvent("Patient_B", 2, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  1, 1), None),
      FlatEvent("Patient_B", 2, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_B", 2, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  3, 1), None),
      FlatEvent("Patient_B", 2, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  4, 1), None),
      // Patient_C exposure 1
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "disease",
        "targetDisease", 1.0, makeTS(2008, 12, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  1, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  3, 1), None),
      // Patient_C exposure 2
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  1, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  2, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "PIOGLITAZONE", 1.0, makeTS(2008,  5, 1), None),
      // Patient_C exposure 3
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  9, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  10, 1), None),
      FlatEvent("Patient_C", 1, makeTS(1940, 1, 1), Some(makeTS(2010, 12, 31)), "molecule",
        "SULFONYLUREA", 1.0, makeTS(2008,  11, 1), None)
    ).toDS

    val expectedResult = Seq(
      CoxFeature("Patient_A", 1, 683, "55-59", 0, 11, 0, 0, 0, 0, 3, 0, 0),
      CoxFeature("Patient_A", 1, 683, "55-59", 11, 21, 0, 0, 3, 0, 3, 0, 0),
      CoxFeature("Patient_A", 1, 683, "55-59", 21, 29, 0, 0, 3, 0, 5, 0, 0),
      CoxFeature("Patient_A", 1, 683, "55-59", 29, 34, 0, 0, 5, 0, 5, 0, 0),
      CoxFeature("Patient_B", 2, 803, "65-69", 0, 17, 0, 0, 0, 0, 2, 0, 0),
      CoxFeature("Patient_C", 1, 803, "65-69", 0, 2, 0, 0, 1, 0, 3, 0, 0),
      CoxFeature("Patient_C", 1, 803, "65-69", 2, 5, 1, 0, 3, 0, 3, 0, 0)
    ).toDF

    // When
    val coxFeatures = CoxMain.coxFeaturing(flatEvents, Map("conf" -> configPath))
    val result = coxFeatures.get.toDF.orderBy("patientID", "start")

    // Then
    flatEvents.toDF.select("patientID", "eventId", "start", "end").orderBy("patientID", "eventId", "start", "end").show
    result.show
    expectedResult.show
    import RichDataFrames._
    assert(result === expectedResult)
  }
}
