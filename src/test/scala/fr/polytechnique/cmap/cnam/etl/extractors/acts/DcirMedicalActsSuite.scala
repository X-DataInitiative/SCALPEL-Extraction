// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.acts

import scala.util.Success
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}
import org.scalatest.TryValues._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

class DcirMedicalActsSuite extends SharedContext {

  import DcirMedicalActExtractor.ColNames

  val schema = StructType(
    StructField(ColNames.PatientID, StringType) ::
      StructField(ColNames.CamCode, StringType) ::
      StructField(ColNames.InstitutionCode, DoubleType) ::
      StructField(ColNames.GHSCode, DoubleType) ::
      StructField(ColNames.Sector, DoubleType) ::
      StructField(ColNames.Date, DateType) :: Nil
  )

  val oldSchema = StructType(
    StructField(ColNames.PatientID, StringType) ::
      StructField(ColNames.CamCode, StringType) ::
      StructField(ColNames.Date, DateType) :: Nil
  )

  "isInStudy" should "return true when a study code is found in the row" in {

    // Given
    val codes = Set("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "AAAA", null, null, null, makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = DcirMedicalActExtractor.isInStudy(codes)(inputRow)

    // Then
    assert(result)
  }

  it should "return false when no code is found in the row" in {

    // Given
    val codes = Set("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "CCCC", 1D, 0D, 1D, makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = DcirMedicalActExtractor.isInStudy(codes)(inputRow)

    // Then
    assert(!result)
  }

  "builder" should "return a DCIR act if the event is in a older version of DCIR" in {
    // Given
    val inputArray = Array[Any]("Patient_A", "AAAA", makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, oldSchema)
    val expected = Seq(DcirAct("Patient_A", DcirAct.groupID.DcirAct, "AAAA", 1.0, makeTS(2010, 1, 1)))

    // When
    val result = DcirMedicalActExtractor.builder(inputRow)

    // Then
    assert(result == expected)
  }

  "getGHS" should "return the value in the correct column" in {
    // Given
    val schema = StructType(StructField(ColNames.GHSCode, DoubleType) :: Nil)
    val inputArray = Array[Any](3D)
    val input = new GenericRowWithSchema(inputArray, schema)
    val expected = 3D

    // When
    val result = DcirMedicalActExtractor.getGHS(input)

    // Then
    assert(result == expected)
  }

  "getSector" should "return the expected value" in {
    // Given
    val schema = StructType(StructField(ColNames.Sector, DoubleType) :: Nil)
    val inputArray = Array[Any](3D)
    val input = new GenericRowWithSchema(inputArray, schema)
    val expected = 3D

    // When
    val result = DcirMedicalActExtractor.getSector(input)

    // Then
    assert(result == expected)
  }

  "getInstitutionCode" should "return the value in the correct column" in {
    // Given
    val schema = StructType(StructField(ColNames.InstitutionCode, DoubleType) :: Nil)
    val inputArray = Array[Any](52D)
    val input = new GenericRowWithSchema(inputArray, schema)
    val expected = 52D

    // When
    val result = DcirMedicalActExtractor.getInstitutionCode(input)

    // Then
    assert(result == expected)

  }

  "getGroupID" should "return correct status of private ambulatory" in {
    // Given
    val schema = StructType(
      StructField(ColNames.GHSCode, DoubleType) ::
        StructField(ColNames.Sector, StringType) ::
        StructField(ColNames.InstitutionCode, DoubleType) :: Nil
    )
    val array = Array[Any](0D, 2D, 6D)
    val input = new GenericRowWithSchema(array, schema)
    val expected = Success(DcirAct.groupID.PrivateAmbulatory)

    // When
    val result = DcirMedicalActExtractor.getGroupId(input)

    // Then
    assert(result == expected)

  }

  it should "return Success(PublicAmbulatory) if it is public related" in {
    // Given
    val schema = StructType(StructField(ColNames.Sector, StringType) :: Nil)
    val array = Array[Any](1D)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirMedicalActExtractor.getGroupId(input)

    // Then
    result.success.value shouldBe DcirAct.groupID.PublicAmbulatory
  }

  it should "return Success(Liberal) if it is liberal act" in {
    // Given
    val schema = StructType(
      StructField(ColNames.Sector, StringType) :: StructField(
        ColNames.GHSCode,
        StringType
      ) :: Nil
    )
    val array = Array[Any](null, null)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirMedicalActExtractor.getGroupId(input)

    // Then
    result.success.value shouldBe DcirAct.groupID.Liberal
  }

  it should "return Success(PrivateAmbulatory) if it is private ambulatory act" in {
    // Given
    val schema = StructType(
      StructField(ColNames.Sector, StringType) :: StructField(
        ColNames.GHSCode,
        DoubleType
      ) :: StructField(ColNames.InstitutionCode, DoubleType) :: Nil
    )
    val array = Array[Any](null, 0D, 4D)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirMedicalActExtractor.getGroupId(input)

    // Then
    result.success.value shouldBe DcirAct.groupID.PrivateAmbulatory
  }

  it should "return Success(UnkownSource) if it is an act with unknown source" in {
    // Given
    val schema = StructType(
      StructField(ColNames.Sector, StringType) :: StructField(
        ColNames.GHSCode,
        DoubleType
      ) :: StructField(ColNames.InstitutionCode, DoubleType) :: Nil
    )
    val array = Array[Any](null, 1D, 4D)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirMedicalActExtractor.getGroupId(input)

    // Then
    result.success.value shouldBe DcirAct.groupID.Unknown
  }

  it should "return IllegalArgumentException if the information of source of act is unavailable in DCIR" in {
    // Given
    val schema = StructType(
      StructField(ColNames.GHSCode, DoubleType) ::
        StructField(ColNames.Sector, StringType) :: Nil
    )
    val array = Array[Any](0D, 2D, 6D)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirMedicalActExtractor.getGroupId(input)

    // Then
    result.failure.exception shouldBe an[IllegalArgumentException]
  }

  "extract" should "return a Dataset of DCIR Medical Acts" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val codes = Set("AAAA", "CCCC")

    val input = Seq(
      ("Patient_A", "AAAA", "NABM1", makeTS(2010, 1, 1), None, None, None),
      ("Patient_A", "BBBB", "NABM1", makeTS(2010, 2, 1), Some(1D), Some(0D), Some(1D)),
      ("Patient_B", "CCCC", "NABM1", makeTS(2010, 3, 1), None, None, None),
      ("Patient_B", "CCCC", "NABM1", makeTS(2010, 4, 1), Some(7D), Some(0D), Some(2D)),
      ("Patient_C", "BBBB", "NABM1", makeTS(2010, 5, 1), Some(1D), Some(0D), Some(2D))
    ).toDF(
      ColNames.PatientID, ColNames.CamCode, ColNames.BioCode, ColNames.Date,
      ColNames.InstitutionCode, ColNames.GHSCode, ColNames.Sector
    )

    val sources = Sources(dcir = Some(input))

    val expected = Seq[Event[MedicalAct]](
      DcirAct("Patient_A", DcirAct.groupID.Liberal, "AAAA", 1.0, makeTS(2010, 1, 1)),
      DcirAct("Patient_B", DcirAct.groupID.Liberal, "CCCC", 1.0, makeTS(2010, 3, 1)),
      DcirAct("Patient_B", DcirAct.groupID.PrivateAmbulatory, "CCCC", 1.0, makeTS(2010, 4, 1))
    ).toDS

    // When
    val result = DcirMedicalActExtractor.extract(sources, codes)

    // Then
    assertDSs(result, expected)
  }
}
