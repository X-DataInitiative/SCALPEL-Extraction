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
    val c = FilteringConfig.getClass.getDeclaredConstructor()
    c.setAccessible(true)
    c.newInstance()
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
    result.printSchema()
    assert(result === expectedResult)
  }

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
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(2010, 12, 31)), "molecule", "insuline", 1.0, makeTS(2009, 1, 20), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "insuline", 1.0, makeTS(2009, 8, 1), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "insuline", 1.0, makeTS(2009, 10, 1), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "C67", 1.0, makeTS(2009, 12, 10), null),
      FlatEvent("Patient_B", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "disease", "targetDisease", 1.0, makeTS(2009, 12, 10), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(2010, 12, 31)), "molecule", "insuline", 1.0, makeTS(2009, 1, 20), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "insuline", 1.0, makeTS(2009, 8, 1), null),
      FlatEvent("Patient_B.1", 2, makeTS(1969, 11, 2), Some(makeTS(1969, 12, 31)), "molecule", "insuline", 1.0, makeTS(2009, 10, 1), null),
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
}
