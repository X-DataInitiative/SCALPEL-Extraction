package fr.polytechnique.cmap.cnam.etl.events.mco

import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, _}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoEventRowExtractorSuite extends SharedContext with McoEventRowExtractor {

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

  "extractCode" should "search a given column for a list of codes and return the found one" in {

    // Given
    val codes = List("C67", "C77")
    val colName: ColName = "Col_Name"
    val schema = StructType(StructField("Col_Name", StringType) :: Nil)
    val row = new GenericRowWithSchema(Array("C779"), schema)
    val expected = Some("C77")

    // When
    val result = extractCode(row, colName, codes)

    // Then
    assert(result == expected)
  }

  it should "return None if no code is found in the given column" in {

    // Given
    val codes = List("C67", "C77")
    val colName: ColName = "Col_Name"
    val schema = StructType(StructField("Col_Name", StringType) :: Nil)
    val row = new GenericRowWithSchema(Array("A99"), schema)
    val expected = None

    // When
    val result = extractCode(row, colName, codes)

    // Then
    assert(result == expected)
  }

  "extractGroupId" should "return the extractGroupId (hospitalization ID for MCO)" in {

    // Given
    val schema = StructType(
      StructField(ColNames.EtaNum, StringType) ::
      StructField(ColNames.RsaNum, StringType) ::
      StructField(ColNames.Year, IntegerType) :: Nil
    )
    val values = Array[Any]("010008407", "0000000793", 2010)
    val row = new GenericRowWithSchema(values, schema)
    val expected = "010008407_0000000793_2010"

    // When
    val result = extractGroupId(row)

    // Then
    assert(result == expected)
  }

  "extractStart" should "compute the start date of the event from the row" in {

    // Given
    val schema = StructType(StructField(NewColumns.EstimatedStayStart, TimestampType) :: Nil)
    val row = new GenericRowWithSchema(Array(makeTS(2010, 1, 1)), schema)
    val expected = makeTS(2010, 1, 1)

    // When
    val result = extractStart(row)

    // Then
    assert(result == expected)
  }
}
