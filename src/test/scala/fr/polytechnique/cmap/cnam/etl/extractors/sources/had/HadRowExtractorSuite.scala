// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.had

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class HadRowExtractorSuite extends SharedContext {

  object MockHadRowExtractor extends HadRowExtractor

  "extractGroupId" should "return the groupID" in {
    // Given
    val schema = StructType(
      Seq(
        StructField("ETA_NUM_EPMSI", StringType),
        StructField("RHAD_NUM", StringType),
        StructField("year", IntegerType)
      )
    )

    val values = Array[Any]("A", "B", 2000)
    val r = new GenericRowWithSchema(values, schema)
    val expected = "A_B_2000"

    // When
    val result = MockHadRowExtractor.extractGroupId(r)

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
    val result = MockHadRowExtractor.extractPatientId(r)

    // Then
    assert(result == expected)
  }

  "extractStart" should "return the start date" in {
    // Given
    val schema = StructType(
      Seq(
        StructField("estimated_start", DateType)
      )
    )

    val values = Array[Any](makeTS(2014, 8, 1))
    val r = new GenericRowWithSchema(values, schema)
    val expected = makeTS(2014, 8, 1)

    // When
    val result = MockHadRowExtractor.extractStart(r)

    // Then
    assert(result == expected)
  }
}