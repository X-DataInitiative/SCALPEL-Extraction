// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.dcir

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class DcirRowExtractorSuite extends SharedContext {

  object MockDcirRowExtractor extends DcirRowExtractor

  "extractGroupId" should "return the groupID" in {
    // Given
    val schema = StructType(
      Seq(
        StructField("FLX_DIS_DTD", StringType),
        StructField("FLX_TRT_DTD", StringType),
        StructField("FLX_EMT_TYP", StringType),
        StructField("FLX_EMT_NUM", StringType),
        StructField("FLX_EMT_ORD", StringType),
        StructField("ORG_CLE_NUM", StringType),
        StructField("DCT_ORD_NUM", StringType)
      )
    )

    val values = Array[Any]("2014-08-01", "2014-07-17", "1", "17", "0", "01C673000", "1749")
    val r = new GenericRowWithSchema(values, schema)
    val expected = "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMwMDBfMTc0OQ=="

    // When
    val result = MockDcirRowExtractor.extractGroupId(r)

    // Then
    assert(result == expected)
  }

  "extractPatientId" should "return the patientId" in {
    // Given
    val schema = StructType(
      Seq(
        StructField("NUM_ENQ", StringType)
      )
    )

    val values = Array[Any]("Patient")
    val r = new GenericRowWithSchema(values, schema)
    val expected = "Patient"

    // When
    val result = MockDcirRowExtractor.extractPatientId(r)

    // Then
    assert(result == expected)
  }

  "extractStart" should "return the start date" in {
    // Given
    val schema = StructType(
      Seq(
        StructField("EXE_SOI_DTD", DateType),
        StructField("FLX_DIS_DTD", DateType)
      )
    )

    val values = Array[Any](makeTS(2014, 8, 1), makeTS(2014, 7, 17))
    val r = new GenericRowWithSchema(values, schema)
    val expected = makeTS(2014, 8, 1)

    // When
    val result = MockDcirRowExtractor.extractStart(r)

    // Then
    assert(result == expected)
  }

  it should "return the flow date when the start date is null" in {
    // Given
    val schema = StructType(
      Seq(
        StructField("EXE_SOI_DTD", DateType),
        StructField("FLX_DIS_DTD", DateType)
      )
    )

    val values = Array[Any](null, makeTS(2014, 7, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = makeTS(2014, 7, 1)

    // When
    val result = MockDcirRowExtractor.extractStart(r)

    // Then
    assert(result == expected)
  }
}