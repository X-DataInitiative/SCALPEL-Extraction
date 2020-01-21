package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, MedicalPractitionerClaim, NonMedicalPractitionerClaim, PractitionerClaimSpeciality}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

import scala.collection.immutable.Stream.Empty

class PractitionerClaimSpecialityExtractorSuite extends SharedContext {

  "extract" should "extract health care related services provided by medical practitioner raw data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val medicalSpeCodes = List("42")
    val input = spark.read.parquet("src/test/resources/test-input/DCIR_w_BIO.parquet")
    val sources = Sources(dcir = Some(input))

    val expected = Seq[Event[PractitionerClaimSpeciality]](
      MedicalPractitionerClaim("Patient_01", "A10000001", "42", makeTS(2006, 2, 1)),
      MedicalPractitionerClaim("Patient_01", "A10000001", "42", makeTS(2006, 1, 15)),
      MedicalPractitionerClaim("Patient_01", "A10000001", "42", makeTS(2006, 1, 30))
    ).toDS


    // When
    val result = MedicalPractitionerClaimExtractor.extract(sources, medicalSpeCodes.toSet)

    // Then
    assertDSs(result, expected)
  }


  "extract" should "extract health care related services provided by non medical practitioner raw data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val nonMedicalSpeCodes = List("42")
    val input = spark.read.parquet("src/test/resources/test-input/DCIR_w_BIO.parquet")
    val sources = Sources(dcir = Some(input))

    val expected = Seq[Event[PractitionerClaimSpeciality]](
      NonMedicalPractitionerClaim("Patient_01", "A10000001", "42", makeTS(2006, 2, 1)),
      NonMedicalPractitionerClaim("Patient_01", "A10000001", "42", makeTS(2006, 1, 15)),
      NonMedicalPractitionerClaim("Patient_01", "A10000001", "42", makeTS(2006, 1, 30)),
      NonMedicalPractitionerClaim("Patient_02", "A10000005", "42", makeTS(2006, 1, 15)),
      NonMedicalPractitionerClaim("Patient_02", "A10000005", "42", makeTS(2006, 1, 30))
    ).toDS


    // When
    val result = NonMedicalPractitionerClaimExtractor.extract(sources, nonMedicalSpeCodes.toSet)

    // Then
    assertDSs(result, expected)
  }

  "extractGroupId" should "return the health care practitioner ID" in {

    // Given
    val schema = StructType(
      StructField("PFS_EXE_NUM", StringType) :: Nil
    )
    val values = Array[Any]("A10000001")
    val row = new GenericRowWithSchema(values, schema)
    val expected = "A10000001"

    // When
    val result = NonMedicalPractitionerClaimExtractor.extractGroupId(row)

    // Then
    assert(result == expected)
  }


  "extract" should "discard providers with a specialty of 0" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val medicalSpeCodes = List()
    val input = spark.read.parquet("src/test/resources/test-input/DCIR_w_BIO.parquet")
    val sources = Sources(dcir = Some(input))

    val expected = Seq[Event[PractitionerClaimSpeciality]](
      MedicalPractitionerClaim("Patient_01", "A10000001", "42", makeTS(2006, 2, 1)),
      MedicalPractitionerClaim("Patient_01", "A10000001", "42", makeTS(2006, 1, 15)),
      MedicalPractitionerClaim("Patient_01", "A10000001", "42", makeTS(2006, 1, 30))
    ).toDS


    // When
    val result = MedicalPractitionerClaimExtractor.extract(sources, medicalSpeCodes.toSet)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "extract health care related services provided by medical practitioner in McoCe" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val medicalSpeCodes = List("1")
    val input = spark.read.parquet("src/test/resources/test-input/MCO_CE.parquet")
    val sources = Sources(mcoCe = Some(input))

    val expected = Seq[Event[PractitionerClaimSpeciality]](
      MedicalPractitionerClaim("2004100010", "390780146_00064268_2014", "1", makeTS(2014, 7, 18))
    ).toDS


    // When
    val result = McoCeSpecialtyExtractor.extract(sources, medicalSpeCodes.toSet)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "extract all health care related services provided by medical practitioner in McoCe" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val medicalSpeCodes = List.empty
    val input = spark.read.parquet("src/test/resources/test-input/MCO_CE.parquet")
    val sources = Sources(mcoCe = Some(input))

    val expected = Seq[Event[PractitionerClaimSpeciality]](
      MedicalPractitionerClaim("2004100010", "390780146_00064268_2014", "1", makeTS(2014, 7, 18)),
      MedicalPractitionerClaim("2004100010", "390780146_00114237_2014", "22", makeTS(2014, 12, 12))
    ).toDS


    // When
    val result = McoCeSpecialtyExtractor.extract(sources, medicalSpeCodes.toSet)

    // Then
    assertDSs(result, expected)
  }
}
