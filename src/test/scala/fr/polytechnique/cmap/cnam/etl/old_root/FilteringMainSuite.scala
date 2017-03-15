package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.functions._

class FilteringMainSuite extends SharedContext {

  override def beforeEach(): Unit = {
    super.beforeEach()
    val c = FilteringConfig.getClass.getDeclaredConstructor()
    c.setAccessible(true)
    c.newInstance()
  }

  "run" should "correctly run the full filtering pipeline for broad cancer definition without exceptions" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/filtering-broad.conf"
    lazy val patientsPath = FilteringConfig.outputPaths.patients
    lazy val flatEventsPath = FilteringConfig.outputPaths.flatEvents
    val expectedPatients: DataFrame = Seq(
      Patient(
        patientID = "Patient_02",
        gender = 1,
        birthDate = makeTS(1959, 10, 1),
        deathDate = Some(makeTS(2008, 1, 25))
      )
    ).toDF

    val expectedFlatEvents: DataFrame = Seq(
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "molecule",
        "PIOGLITAZONE", 840.0, makeTS(2006, 1, 15), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "molecule",
        "PIOGLITAZONE", 4200.0, makeTS(2006, 1, 30), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "molecule",
        "PIOGLITAZONE", 1680.0, makeTS(2006, 1, 5), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2006, 3, 13), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2005, 12, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2005, 12, 24), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2008, 3, 8), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2008, 3, 15), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2007, 1, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2007, 1, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2006, 3, 13), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2005, 12, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2005, 12, 24), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2008, 3, 8), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2008, 3, 15), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2007, 1, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2007, 1, 29), None)  // duplicate event, it's ok. See the
      // Scaladoc of McoDiseaseTransformer.estimateStayStartTime for explanation.
    ).toDF

    // When
    FilteringMain.run(sqlContext, Map("conf" -> configPath))

    // Then
    val patients = sqlCtx.read.parquet(patientsPath)
    val flatEvents = sqlCtx.read.parquet(flatEventsPath)
    assertDFs(patients, expectedPatients)
    assertDFs(flatEvents, expectedFlatEvents)
 }


  // We have a problem in this test because FilteringConfig was already instantiated by the previous
  //   one with another configuration file.
  // This will happen again whenever we want to test different values in the configuration, which I
  //   think will be frequent. The only solution I can think of is using "vars" in FilteringConfig
  //   and using a setter method for updating the config (i.e. Java-like strategy).
  it should "correctly run the full filtering pipeline for narrow cancer definition without exceptions" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/filtering-narrow.conf"
    lazy val patientsPath = FilteringConfig.outputPaths.patients
    lazy val flatEventsPath = FilteringConfig.outputPaths.flatEvents
    val expectedPatients: DataFrame = Seq(
      Patient(
        patientID = "Patient_02",
        gender = 1,
        birthDate = makeTS(1959, 10, 1),
        deathDate = Some(makeTS(2008, 1, 25))
      )
    ).toDF

    val expectedFlatEvents: DataFrame = Seq(
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "molecule",
        "PIOGLITAZONE", 840.0, makeTS(2006, 1, 15), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "molecule",
        "PIOGLITAZONE", 4200.0, makeTS(2006, 1, 30), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "molecule",
        "PIOGLITAZONE", 1680.0, makeTS(2006, 1, 5), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2006, 3, 13), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2005, 12, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2005, 12, 24), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2008, 3, 8), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2008, 3, 15), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2007, 1, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2007, 1, 29), None)  // duplicate event, it's ok. See the
      // Scaladoc of McoDiseaseTransformer.estimateStayStartTime for explanation.
    ).toDF

    // When
    FilteringMain.run(sqlContext, Map("conf" -> configPath))

    // Then
    val patients = sqlCtx.read.parquet(patientsPath)
    val flatEvents = sqlCtx.read.parquet(flatEventsPath)
      assertDFs(patients, expectedPatients)
     assertDFs(flatEvents, expectedFlatEvents)
 }

  it should "return a saved flatEventsDataset if reuseFlatEventsPath is defined" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/filtering-reuse-flat-events.conf"

    val expected: DataFrame = Seq(
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "molecule",
        "PIOGLITAZONE", 840.0, makeTS(2006, 1, 15), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "molecule",
        "PIOGLITAZONE", 4200.0, makeTS(2006, 1, 30), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "molecule",
        "PIOGLITAZONE", 1680.0, makeTS(2006, 1, 5), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2006, 3, 13), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2005, 12, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2005, 12, 24), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2008, 3, 8), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2008, 3, 15), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2007, 1, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "targetDisease", 1.0, makeTS(2007, 1, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2006, 3, 13), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2005, 12, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2005, 12, 24), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2008, 3, 8), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2008, 3, 15), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2007, 1, 29), None),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "disease",
        "C67", 1.0, makeTS(2007, 1, 29), None)  // duplicate event, it's ok. See the
      // Scaladoc of McoDiseaseTransformer.estimateStayStartTime for explanation.
    ).toDF

    // When
    val result = FilteringMain.run(sqlContext, Map("conf" -> configPath)).get.toDF

    // Then
    assertDFs(result, expected)
  }
}