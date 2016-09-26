package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import com.typesafe.config.ConfigFactory
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class MainSuite extends SharedContext {

  val config = ConfigFactory.parseResources("filtering.conf").getConfig("test")
  val patientsPath = config.getString("paths.output.patients")
  val eventsPath = config.getString("paths.output.events")
  val exposuresPath = config.getString("paths.output.exposures")
  val coxPath = config.getString("paths.output.coxFeatures")
  val LTSCSSPath = config.getString("paths.output.LTSCCSFeatures")

  "runETL" should "correctly run the full filtering pipeline without exceptions" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val expectedPatients: DataFrame = Seq(
      Patient(
        patientID = "Patient_02",
        gender = 1,
        birthDate = makeTS(1959, 10, 1),
        deathDate = Some(makeTS(2008, 1, 25))
      )
    ).toDF

    val expectedFlatEvents: DataFrame = Seq(
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "trackloss",
        "eventId", 1.0, makeTS(2006, 3, 30), None),
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
        "C67", 1.0, makeTS(2007, 1, 29), None),  // duplicate event, it's ok. See the
      // Scaladoc of McoDiseaseTransformer.estimateStayStartTime for explanation.
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)), "followUpPeriod",
        "disease", 1.0, makeTS(2006, 7, 5), Some(makeTS(2005, 12, 24))),
      FlatEvent("Patient_02", 1, makeTS(1959, 10, 1), Some(makeTS(2008, 1, 25)),
        "observationPeriod",  "observationPeriod", 1.0, makeTS(2006, 1, 5),
        Some(Timestamp.valueOf("2009-12-31 23:59:59")))
    ).toDF

    // When
    FilteringMain.runETL(sqlContext, config)

    // Then
    val patients = sqlCtx.read.parquet(patientsPath)
    val events = sqlCtx.read.parquet(eventsPath)
    patients.show
    expectedPatients.show
    events.orderBy("patientID", "category", "start").show(50)
    expectedFlatEvents.orderBy("patientID", "category", "start").show(50)

    import RichDataFrames._
    assert(patients === expectedPatients)
    assert(events === expectedFlatEvents)
  }
}