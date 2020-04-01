// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.ssrce

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class SsrCeRowExtractorSuite extends SharedContext {

  object MockSsrCeRowExtractor extends SsrCeRowExtractor

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
    val result = MockSsrCeRowExtractor.extractPatientId(r)

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
    val result = MockSsrCeRowExtractor.extractStart(r)

    // Then
    assert(result == expected)
  }
}