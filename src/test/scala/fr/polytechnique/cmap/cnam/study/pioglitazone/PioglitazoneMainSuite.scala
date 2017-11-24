package fr.polytechnique.cmap.cnam.study.pioglitazone

/*
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.study.StudyConfig
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql._

class PioglitazoneMainSuite extends SharedContext{

  "run" should "run the whole pipeline with MLPP featuring" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val expectedPatients: DataFrame = Seq[Patient](
      Patient(
        patientID = "Patient_02",
        gender = 1,
        birthDate = makeTS(1959, 10, 1),
        deathDate = Some(makeTS(2008, 1, 25))
      ),
      Patient(
        patientID = "Patient_01",
        gender = 2,
        birthDate = makeTS(1975, 1, 1),
        deathDate = None
      )
    ).toDF

    val expectedFlatEvents: DataFrame = Seq[Event[AnyEvent]](
      Molecule("Patient_01", "SULFONYLUREA", 1800.0, makeTS(2006, 1, 15)),
      Molecule("Patient_02", "PIOGLITAZONE", 840.0, makeTS(2006, 1, 15)),
      Molecule("Patient_02", "PIOGLITAZONE", 4200.0, makeTS(2006, 1, 30)),
      Molecule("Patient_02", "PIOGLITAZONE", 1680.0, makeTS(2006, 1, 5)),
      MainDiagnosis("Patient_02", "10000123_20000123_2007", "C67", makeTS(2007,  1, 29)),
      MainDiagnosis("Patient_02", "10000123_20000345_2007", "C67", makeTS(2007,  1, 29)),
      MainDiagnosis("Patient_02", "10000123_10000987_2006", "C67", makeTS(2005, 12, 29)),
      MainDiagnosis("Patient_02", "10000123_10000543_2006", "C67", makeTS(2005, 12, 24)),
      MainDiagnosis("Patient_02", "10000123_30000546_2008", "C67", makeTS(2008,  3,  8)),
      MainDiagnosis("Patient_02", "10000123_30000852_2008", "C67", makeTS(2008,  3, 15)),
      ImbDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13))
    ).toDF
    val expectedMLPPFeatures: DataFrame = Seq(
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 3, 0, 3, 0, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 4, 1, 4, 1, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 5, 2, 5, 2, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 6, 3, 6, 3, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 7, 4, 7, 4, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 8, 5, 8, 5, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 9, 6, 9, 6, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 10, 7, 10, 7, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 11, 8, 11, 8, 1.0),
      MLPPFeature("Patient_02", 0, "PIOGLITAZONE", 0, 12, 9, 12, 9, 1.0)
    ).toDF
    // When
    val configPath = "src/test/resources/config/filtering-broad.conf"
    PioglitazoneMain.run(sqlContext, Map("conf" -> configPath, "env" -> "test", "study" -> "pioglitazone"))
    val outputPaths = StudyConfig.outputPaths

    // Then
    val patients = sqlCtx.read.parquet(outputPaths.patients)
    val flatEvents = sqlCtx.read.parquet(outputPaths.flatEvents)

    val mlppSparseFeatures = sqlCtx.read.parquet(outputPaths.mlppFeatures + "/parquet/SparseFeatures")

    assertDFs(patients, expectedPatients)
    assertDFs(flatEvents, expectedFlatEvents)
    assertDFs(mlppSparseFeatures, expectedMLPPFeatures)

  }

}
*/
