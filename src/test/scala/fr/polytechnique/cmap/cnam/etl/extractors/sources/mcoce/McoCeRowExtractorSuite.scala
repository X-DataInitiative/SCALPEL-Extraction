// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.mcoce

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoCeRowExtractorSuite extends SharedContext {

  object MockMcoCeRowExtractor extends McoCeRowExtractor

  "extractGroupId" should "return the groupID" in {
    // Given
    val schema = StructType(
      Seq(
        StructField("ETA_NUM", StringType),
        StructField("SEQ_NUM", StringType),
        StructField("year", IntegerType)
      )
    )

    val values = Array[Any]("A", "B", 2000)
    val r = new GenericRowWithSchema(values, schema)
    val expected = "A_B_2000"

    // When
    val result = MockMcoCeRowExtractor.extractGroupId(r)

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
    val result = MockMcoCeRowExtractor.extractPatientId(r)

    // Then
    assert(result == expected)
  }

  "extractStart" should "return the start date" in {
    // Given
    val schema = StructType(
      Seq(
        StructField("EXE_SOI_DTD", DateType)
      )
    )

    val values = Array[Any](makeTS(2014, 8, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = makeTS(2014, 8, 1)

    // When
    val result = MockMcoCeRowExtractor.extractStart(r)

    // Then
    assert(result == expected)
  }
}