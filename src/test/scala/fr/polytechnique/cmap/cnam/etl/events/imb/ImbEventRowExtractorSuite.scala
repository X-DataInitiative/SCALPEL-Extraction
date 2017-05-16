package fr.polytechnique.cmap.cnam.etl.events.imb

import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, _}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class ImbEventRowExtractorSuite extends SharedContext with ImbEventRowExtractor {

  "extractPatientId" should "return the patientID value of a row" in {

    // Given
    val schema = StructType(StructField(ColNames.PatientID, StringType) :: Nil)
    val row = new GenericRowWithSchema(Array("Patient_A"), schema)
    val expected = "Patient_A"

    // When
    val result = extractPatientId(row)

    // Then
    assert(result == expected)
  }

  "extractCode" should "search the correct column for a list of codes and return the found one" in {

    // Given
    val codes = List("C67", "C77")
    val schema = StructType(
      StructField(ColNames.Encoding, StringType) :: StructField(ColNames.Code, StringType) :: Nil
    )
    val row = new GenericRowWithSchema(Array("CIM10", "C779"), schema)
    val expected = Some("C77")

    // When
    val result = extractCode(row, codes)

    // Then
    assert(result == expected)
  }

  it should "return None if no code is found" in {

    // Given
    val codes = List("C67", "C77")
    val schema = StructType(
      StructField(ColNames.Encoding, StringType) :: StructField(ColNames.Code, StringType) :: Nil
    )
    val row = new GenericRowWithSchema(Array("CIM10", "A99"), schema)
    val expected = None

    // When
    val result = extractCode(row, codes)

    // Then
    assert(result == expected)
  }

  it should "return None if the encoding is not CIM10" in {

    // Given
    val codes = List("C67", "C77")
    val schema = StructType(
      StructField(ColNames.Encoding, StringType) :: StructField(ColNames.Code, StringType) :: Nil
    )
    val row = new GenericRowWithSchema(Array("Other_Encoding", "C779"), schema)
    val expected = None

    // When
    val result = extractCode(row, codes)

    // Then
    assert(result == expected)
  }


  "start" should "return the event date" in {

    // Given
    val schema = StructType(StructField(ColNames.Date, TimestampType) :: Nil)
    val row = new GenericRowWithSchema(Array(makeTS(2010, 1, 1)), schema)
    val expected = makeTS(2010, 1, 1)

    // When
    val result = extractStart(row)

    // Then
    assert(result == expected)
  }
}
