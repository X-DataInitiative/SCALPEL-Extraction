package fr.polytechnique.cmap.cnam.etl.events.mco

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.{AssociatedDiagnosis, LinkedDiagnosis, MainDiagnosis}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.util.functions._

class McoCodeEventsExtractorSuite extends SharedContext with McoSource {

  "eventsFromRow" should "create all code events found in a row" in {

    // Given
    val codesMap = Map(
      ColNames.DP -> List("C67", "C77", "C78"),
      ColNames.DR -> List("C67", "C77"),
      ColNames.DA -> List("C67")
    )

    val schema = StructType(
      StructField(ColNames.PatientID, StringType) ::
      StructField(ColNames.EtaNum, StringType) ::
      StructField(ColNames.RsaNum, StringType) ::
      StructField(ColNames.Year, IntegerType) ::
      StructField(ColNames.DP, StringType) ::
      StructField(ColNames.DR, StringType) ::
      StructField(ColNames.DA, StringType) ::
      StructField(NewColumns.EstimatedStayStart, TimestampType) :: Nil
    )

    val values = Array[Any](
      "Patient_A",
      "1",
      "1",
      2010,
      "C78",
      "C77",
      "C67",
      makeTS(2010, 1, 1)
    )

    val row = new GenericRowWithSchema(values, schema)

    val expected = List[Event[AnyEvent]](
      MainDiagnosis("Patient_A", "1_1_2010", "C78", makeTS(2010, 1, 1)),
      LinkedDiagnosis("Patient_A", "1_1_2010", "C77", makeTS(2010, 1, 1)),
      AssociatedDiagnosis("Patient_A", "1_1_2010", "C67", makeTS(2010, 1, 1))
    )

    // When
    val result: List[Event[AnyEvent]] = McoCodeEventsExtractor.eventsFromRow(codesMap)(row)

    // Then
    assert(result == expected)
  }

  "extract" should "create a Dataset from the mco source" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given

    val config = ExtractionConfig.init().copy(
      codesMap = Map(
        "dp" -> List("C67"),
        "dr" -> List("E05"),
        "da" -> List("C66")
      )
    )
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")

    val expected = Seq(
      MainDiagnosis("Patient_02", "10000123_20000123_2007", "C67", makeTS(2007, 1, 29)),
      LinkedDiagnosis("Patient_02", "10000123_20000123_2007", "E05", makeTS(2007, 1, 29)),
      AssociatedDiagnosis("Patient_02", "10000123_20000123_2007", "C66", makeTS(2007, 1, 29)),
      MainDiagnosis("Patient_02", "10000123_20000345_2007", "C67", makeTS(2007, 1, 29)),
      MainDiagnosis("Patient_02", "10000123_10000987_2006", "C67", makeTS(2005, 12, 29)),
      MainDiagnosis("Patient_02", "10000123_10000543_2006", "C67", makeTS(2005, 12, 24)),
      AssociatedDiagnosis("Patient_02", "10000123_10000543_2006", "C66", makeTS(2005, 12, 24)),
      MainDiagnosis("Patient_02", "10000123_30000546_2008", "C67", makeTS(2008, 3, 8)),
      MainDiagnosis("Patient_02", "10000123_30000852_2008", "C67", makeTS(2008, 3, 15))
    )

    // When
    val result = McoCodeEventsExtractor.extract(config, mco)

    // Then
    assertDFs(result.toDF, expected.toDF, true)
  }
}
