package fr.polytechnique.cmap.cnam.etl.extractors.dcir

import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, _}
import org.scalatest.FlatSpec

trait DcirEventRowExtractorSuite extends FlatSpec with DcirEventRowExtractor {

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
    val codes = List("42", "43")
    val colName: ColName = "Col_Name"
    val schema = StructType(StructField("Col_Name", StringType) :: Nil)
    val row = new GenericRowWithSchema(Array("42", "420"), schema)
    val expected = Some("42")

    // When
    val result = extractCode(row, colName, codes)

    // Then
    assert(result == expected)
  }

  it should "return None if no code is found in the given column" in {

    // Given
    val codes = List("42", "43")
    val colName: ColName = "Col_Name"
    val schema = StructType(StructField("Col_Name", StringType) :: Nil)
    val row = new GenericRowWithSchema(Array("21"), schema)
    val expected = None

    // When
    val result = extractCode(row, colName, codes)

    // Then
    assert(result == expected)
  }

  "extractGroupId" should "return the extractGroupId (healthcare practionner ID for PrestationSpeciality)" in {

    // Given
    val schema = StructType(
      StructField(ColNames.ExecPSNum, StringType) :: Nil
    )
    val values = Array[Any]("A10000001")
    val row = new GenericRowWithSchema(values, schema)
    val expected = "A10000001"

    // When
    val result = extractGroupId(row)

    // Then
    assert(result == expected)
  }

  "extractStart" should "compute the start date of the event from the row" in {

    // Given
    val schema = StructType(StructField(ColNames.PrestaStart, TimestampType) :: Nil)
    val row = new GenericRowWithSchema(Array(makeTS(2010, 1, 1)), schema)
    val expected = makeTS(2010, 1, 1)

    // When
    val result = extractStart(row)

    // Then
    assert(result == expected)
  }

}
