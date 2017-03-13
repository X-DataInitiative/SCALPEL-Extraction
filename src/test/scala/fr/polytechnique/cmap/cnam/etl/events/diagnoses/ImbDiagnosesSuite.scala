package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class ImbDiagnosesSuite extends SharedContext {

  "rowToDiagnoses" should "convert a row to a list of imb diagnosis events" in {

    // Given
    val codes = List("C67", "C68")

    val schema = StructType(
      StructField("patientID", StringType) ::
      StructField("diagnosis_code", StringType) ::
      StructField("event_date", TimestampType) :: Nil
    )

    val rows = List(
      Array[Any]("Patient01", "C67", makeTS(2010, 1, 1)),
      Array[Any]("Patient01", "noiseC67noiseC68", makeTS(2010, 1, 1)),
      Array[Any]("Patient01", "noiseC67noise", makeTS(2010, 2, 1)),
      Array[Any]("Patient01", "noise", makeTS(2010, 3, 1))
    ).map(new GenericRowWithSchema(_, schema))

    val expected = List[List[Event[Diagnosis]]](
      List(ImbDiagnosis("Patient01", "C67", makeTS(2010, 1, 1))),
      List(
        ImbDiagnosis("Patient01", "C67", makeTS(2010, 1, 1)),
        ImbDiagnosis("Patient01", "C68", makeTS(2010, 1, 1))
      ),
      List(ImbDiagnosis("Patient01", "C67", makeTS(2010, 2, 1))),
      List[Event[Diagnosis]]()
    )

    // When
    val result = rows.map(ImbDiagnoses.rowToDiagnoses(codes))

    // Then
    assert(result == expected)
  }

  "extract" should "extract diagnosis events from raw data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExtractionConfig.init().copy(imbDiagnosisCodes = List("C67"))
    val input = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R.parquet")
    val expected = Seq(ImbDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13))).toDF

    // When
    val output = ImbDiagnoses.extract(config, input)

    // Then
    assertDFs(expected.toDF, output.toDF, true)
  }
}
