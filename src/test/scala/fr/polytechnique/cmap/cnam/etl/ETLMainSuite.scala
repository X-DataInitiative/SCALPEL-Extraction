package fr.polytechnique.cmap.cnam.etl

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.etl.events.molecules._
import fr.polytechnique.cmap.cnam.etl.events.diagnoses._
import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.functions._

class ETLMainSuite extends SharedContext {

  override def beforeEach(): Unit = {
    super.beforeEach()
    val c = FilteringConfig.getClass.getDeclaredConstructor()
    c.setAccessible(true)
    c.newInstance()
  }

  "run" should "correctly run the full filtering pipeline without exceptions" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/filtering-broad.conf"
    lazy val patientsPath = FilteringConfig.outputPaths.patients
    lazy val flatEventsPath = FilteringConfig.outputPaths.flatEvents
    val expectedPatients: DataFrame = Seq[Patient](
      Patient(
        patientID = "Patient_02",
        gender = 1,
        birthDate = makeTS(1959, 10, 1),
        deathDate = Some(makeTS(2008, 1, 25))
      )
    ).toDF

    val expectedFlatEvents: DataFrame = Seq[Event[AnyEvent]](
      Molecule("Patient_01", "SULFONYLUREA", 1800.0, makeTS(2006, 1, 15)),
      Molecule("Patient_02", "PIOGLITAZONE", 840.0, makeTS(2006, 1, 15)),
      Molecule("Patient_02", "PIOGLITAZONE", 4200.0, makeTS(2006, 1, 30)),
      Molecule("Patient_02", "PIOGLITAZONE", 1680.0, makeTS(2006, 1, 5)),
      MainDiagnosis("Patient_02", "C67", makeTS(2007,  1, 29)),
      MainDiagnosis("Patient_02", "C67", makeTS(2007,  1, 29)),
      MainDiagnosis("Patient_02", "C67", makeTS(2005, 12, 29)),
      MainDiagnosis("Patient_02", "C67", makeTS(2005, 12, 24)),
      MainDiagnosis("Patient_02", "C67", makeTS(2008,  3,  8)),
      MainDiagnosis("Patient_02", "C67", makeTS(2008,  3, 15)),
      ImbDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13))
    ).toDF

    // When
    ETLMain.run(sqlContext, Map("conf" -> configPath))

    // Then
    val patients = sqlCtx.read.parquet(patientsPath)
    val flatEvents = sqlCtx.read.parquet(flatEventsPath)
    assertDFs(patients, expectedPatients)
    assertDFs(flatEvents, expectedFlatEvents)
 }

  it should "return a saved events Dataset if reuseFlatEventsPath is defined" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val configPath = "src/test/resources/config/filtering-reuse-events.conf"

    val expected: DataFrame = Seq[Event[AnyEvent]](
      Molecule("Patient_01", "SULFONYLUREA", 1800.0, makeTS(2006, 1, 15)),
      Molecule("Patient_02", "PIOGLITAZONE", 840.0, makeTS(2006, 1, 15)),
      Molecule("Patient_02", "PIOGLITAZONE", 4200.0, makeTS(2006, 1, 30)),
      Molecule("Patient_02", "PIOGLITAZONE", 1680.0, makeTS(2006, 1, 5)),
      MainDiagnosis("Patient_02", "C67", makeTS(2007,  1, 29)),
      MainDiagnosis("Patient_02", "C67", makeTS(2007,  1, 29)),
      MainDiagnosis("Patient_02", "C67", makeTS(2005, 12, 29)),
      MainDiagnosis("Patient_02", "C67", makeTS(2005, 12, 24)),
      MainDiagnosis("Patient_02", "C67", makeTS(2008,  3,  8)),
      MainDiagnosis("Patient_02", "C67", makeTS(2008,  3, 15)),
      ImbDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13))
    ).toDF

    // When
    val result = ETLMain.run(sqlContext, Map("conf" -> configPath)).get.toDF

    // Then
    assertDFs(result, expected, true)
  }
}