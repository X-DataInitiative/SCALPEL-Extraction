package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class DcirPrestationsExtractorSuite extends SharedContext {

  "extract" should "extract prestations events from raw data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val medicalSpeCodes = List("42")
    val nonMedicalSpeCodes = List("42")
    val input = spark.read.parquet("src/test/resources/test-input/DCIR.parquet")

    val expected = Seq[Event[PrestationSpeciality]](
      MedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 2, 1)),
      MedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 1, 15)),
      MedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 1, 30)),
      NonMedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 2, 1)),
      NonMedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 1, 15)),
      NonMedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 1, 30)),
      NonMedicalPrestation("Patient_02", "A10000005", "42", makeTS(2006, 1, 5)),
      NonMedicalPrestation("Patient_02", "A10000005", "42", makeTS(2006, 1, 15)),
      NonMedicalPrestation("Patient_02", "A10000005", "42", makeTS(2006, 1, 30))
    ).toDS

    // When
    val result = DcirPrestationsExtractor(medicalSpeCodes, nonMedicalSpeCodes).extract(input)

    // Then
    assertDSs(result, expected)
  }

  "extractGroupId" should "return the extractGroupId (healthcare practionner ID for PrestationSpeciality)" in {
    val medicalSpeCodes = List("42")
    val nonMedicalSpeCodes = List("42")
    // Given
    val schema = StructType(
      StructField("PFS_EXE_NUM", StringType) :: Nil
    )
    val values = Array[Any]("A10000001")
    val row = new GenericRowWithSchema(values, schema)
    val expected = "A10000001"

    // When
    val result =  DcirPrestationsExtractor(medicalSpeCodes, nonMedicalSpeCodes).extractGroupId(row)

    // Then
    assert(result == expected)
  }
}

