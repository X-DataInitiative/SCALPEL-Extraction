package fr.polytechnique.cmap.cnam.etl.events.imb

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.ImbDiagnosis
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.util.functions._

class ImbCodeEventsExtractorSuite extends SharedContext with ImbSource {

  "eventsFromRow" should "create a code events found in a row" in {

    // Given
    val codes = List("C67", "C77")

    val schema = StructType(
      StructField(ColNames.PatientID, StringType) ::
      StructField(ColNames.Encoding, StringType) ::
      StructField(ColNames.Code, StringType) ::
      StructField(ColNames.Date, TimestampType) :: Nil
    )

    val values = Array[Any]("Patient_A", "CIM10", "C77", makeTS(2010, 1, 1))

    val row = new GenericRowWithSchema(values, schema)

    val expected = List[Event[AnyEvent]](
      ImbDiagnosis("Patient_A", "NA", "C77", makeTS(2010, 1, 1))
    )

    // When
    val result: List[Event[AnyEvent]] = ImbCodeEventsExtractor.eventsFromRow(codes)(row)

    // Then
    assert(result == expected)
  }

  it should "return an empty list if no code events is found in a row" in {

    // Given
    val codes = List("C67", "C77")

    val schema = StructType(
      StructField(ColNames.PatientID, StringType) ::
      StructField(ColNames.Encoding, StringType) ::
      StructField(ColNames.Code, StringType) ::
      StructField(ColNames.Date, TimestampType) :: Nil
    )

    val values1 = Array[Any]("Patient_A", "Other_Encoding", "C77", makeTS(2010, 1, 1))
    val values2 = Array[Any]("Patient_A", "CIM10", "A00", makeTS(2010, 1, 1))

    val row1 = new GenericRowWithSchema(values1, schema)
    val row2 = new GenericRowWithSchema(values2, schema)

    val expected = List[Event[AnyEvent]]()

    // When
    val result1: List[Event[AnyEvent]] = ImbCodeEventsExtractor.eventsFromRow(codes)(row1)
    val result2: List[Event[AnyEvent]] = ImbCodeEventsExtractor.eventsFromRow(codes)(row2)

    // Then
    assert(result1 == expected)
    assert(result2 == expected)
  }

  "extract" should "create a Dataset from the mco source" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given

    val config = ExtractionConfig.init().copy(
      codesMap = Map(
        "imb" -> List("E11", "C67")
      )
    )
    val imb = spark.read.parquet("src/test/resources/test-input/IR_IMB_R.parquet")

    val expected = Seq(
      ImbDiagnosis("Patient_02", "NA", "E11", makeTS(2006, 1, 25)),
      ImbDiagnosis("Patient_02", "NA", "C67", makeTS(2006, 3, 13))
    )

    // When
    val result = ImbCodeEventsExtractor.extract(config, imb)

    // Then
    assertDFs(result.toDF, expected.toDF)
  }
}
