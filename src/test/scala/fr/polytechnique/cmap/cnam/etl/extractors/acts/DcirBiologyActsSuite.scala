package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{BiologyAct, BiologyDcirAct, DcirAct, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.Matchers.{an, convertToAnyShouldWrapper}
import org.scalatest.TryValues._

import scala.util.Success

class DcirBiologyActsSuite extends SharedContext {

  import DcirBiologyActExtractor.ColNames

  val schema = StructType(
    StructField(ColNames.PatientID, StringType) ::
      StructField(ColNames.BioCode, StringType) ::
      StructField(ColNames.InstitutionCode, DoubleType) ::
      StructField(ColNames.GHSCode, DoubleType) ::
      StructField(ColNames.Sector, DoubleType) ::
      StructField(ColNames.Date, DateType) :: Nil
  )

  val oldSchema = StructType(
    StructField(ColNames.PatientID, StringType) ::
      StructField(ColNames.BioCode, StringType) ::
      StructField(ColNames.Date, DateType) :: Nil
  )

  "isInStudy" should "return true when a study code is found in the row" in {

    // Given
    val codes = Set("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "AAAA", null, null, null, makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = DcirBiologyActExtractor.isInStudy(codes)(inputRow)

    // Then
    assert(result)
  }

  it should "return false when no code is found in the row" in {

    // Given
    val codes = Set("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "CCCC", 1D, 0D, 1D, makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = DcirBiologyActExtractor.isInStudy(codes)(inputRow)

    // Then
    assert(!result)
  }

  "builder" should "return a DCIR act if the event is in a older version of DCIR" in {
    // Given
    val inputArray = Array[Any]("Patient_A", "AAAA", makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, oldSchema)
    val expected = Seq(BiologyDcirAct("Patient_A", BiologyDcirAct.groupID.DcirAct, "AAAA", makeTS(2010, 1, 1)))

    // When
    val result = DcirBiologyActExtractor.builder(inputRow)

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
    val result = DcirBiologyActExtractor.getGHS(input)

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
    val result = DcirBiologyActExtractor.getSector(input)

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
    val result = DcirBiologyActExtractor.getInstitutionCode(input)

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
    val result = DcirBiologyActExtractor.getGroupId(input)

    // Then
    assert(result == expected)

  }

  it should "return Success(PublicAmbulatory) if it is public related" in {
    // Given
    val schema = StructType(StructField(ColNames.Sector, StringType) :: Nil)
    val array = Array[Any](1D)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirBiologyActExtractor.getGroupId(input)

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
    val result = DcirBiologyActExtractor.getGroupId(input)

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
    val result = DcirBiologyActExtractor.getGroupId(input)

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
    val result = DcirBiologyActExtractor.getGroupId(input)

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
    val result = DcirBiologyActExtractor.getGroupId(input)

    // Then
    result.failure.exception shouldBe an[IllegalArgumentException]
  }

  "extract" should "return a Dataset of DCIR Biology Acts" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val codes = Set("AAAA", "CCCC")

    val input = Seq(
      ("Patient_A", "AAAA", Some(makeTS(2010, 1, 1)), None, None, None, makeTS(2010, 1, 1)),
      ("Patient_A", "BBBB", Some(makeTS(2010, 2, 1)), Some(1D), Some(0D), Some(1D), makeTS(2010, 2, 1)),
      ("Patient_B", "CCCC", Some(makeTS(2010, 3, 1)), None, None, None, makeTS(2010, 3, 1)),
      ("Patient_B", "CCCC", Some(makeTS(2010, 4, 1)), Some(7D), Some(0D), Some(2D), makeTS(2010, 4, 1)),
      ("Patient_C", "BBBB", None, Some(1D), Some(0D), Some(2D), makeTS(2010, 5, 1))
    ).toDF(
      ColNames.PatientID, ColNames.BioCode, ColNames.Date,
      ColNames.InstitutionCode, ColNames.GHSCode, ColNames.Sector, ColNames.DcirFluxDate
    )

    val sources = Sources(dcir = Some(input))

    val expected = Seq[Event[BiologyAct]](
      BiologyDcirAct("Patient_A", BiologyDcirAct.groupID.Liberal, "AAAA", makeTS(2010, 1, 1)),
      BiologyDcirAct("Patient_B", BiologyDcirAct.groupID.Liberal, "CCCC", makeTS(2010, 3, 1)),
      BiologyDcirAct("Patient_B", BiologyDcirAct.groupID.PrivateAmbulatory, "CCCC", makeTS(2010, 4, 1))
    ).toDS

    // When
    val result = DcirBiologyActExtractor.extract(sources, codes)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return a Dataset of DCIR Biology Acts from raw data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val codes = Set("238")

    val input = sqlCtx.read.parquet("src/test/resources/test-input/DCIR.parquet")

    val sources = Sources(dcir = Some(input))

    val expected = Seq[Event[BiologyAct]](
      BiologyDcirAct("Patient_01", BiologyDcirAct.groupID.Liberal, "238", makeTS(2006, 1, 15))
    ).toDS

    // When
    val result = DcirBiologyActExtractor.extract(sources, codes)

    // Then
    assertDSs(result, expected)
  }
}
