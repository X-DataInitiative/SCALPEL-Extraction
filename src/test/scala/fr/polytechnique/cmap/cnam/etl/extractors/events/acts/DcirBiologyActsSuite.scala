package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{BiologyDcirAct, DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.dcir.DcirSource
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS


class DcirBiologyActsSuite extends SharedContext {

  val colNames = new DcirSource {}.ColNames

  val schema = StructType(
    StructField(colNames.PatientID, StringType) ::
      StructField(colNames.BioCode, StringType) ::
      StructField(colNames.InstitutionCode, DoubleType) ::
      StructField(colNames.GHSCode, DoubleType) ::
      StructField(colNames.Sector, DoubleType) ::
      StructField(colNames.FlowDistributionDate, DateType) :: Nil
  )

  val oldSchema = StructType(
    StructField(colNames.PatientID, StringType) ::
      StructField(colNames.BioCode, StringType) ::
      StructField(colNames.FlowDistributionDate, DateType) :: Nil
  )

  it should "return false when no code is found in the row" in {

    // Given
    val codes = SimpleExtractorCodes(List("AAAA", "BBBB"))
    val inputArray = Array[Any]("Patient_A", "CCCC", 1D, 0D, 1D, makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = DcirBiologyActExtractor(codes).isInStudy(inputRow)

    // Then
    assert(!result)
  }

  "getGroupID" should "return correct status of private ambulatory" in {
    // Given
    val schema = StructType(
      StructField(colNames.GHSCode, DoubleType) ::
        StructField(colNames.Sector, StringType) ::
        StructField(colNames.InstitutionCode, DoubleType) :: Nil
    )
    val array = Array[Any](0D, 2D, 6D)
    val input = new GenericRowWithSchema(array, schema)

    // When
    val result = DcirBiologyActExtractor(SimpleExtractorCodes(List("AAAA", "BBBB"))).extractGroupId(input)

    // Then
    assert(result == DcirAct.groupID.PrivateAmbulatory)

  }

  it should "return Success(PublicAmbulatory) if it is public related" in {
    // Given
    val schema = StructType(StructField(colNames.Sector, StringType) :: Nil)
    val array = Array[Any](1D)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirBiologyActExtractor(SimpleExtractorCodes.empty).extractGroupId(input)

    // Then
    assert(result == DcirAct.groupID.PublicAmbulatory)
  }

  it should "return Success(Liberal) if it is liberal act" in {
    // Given
    val schema = StructType(
      StructField(colNames.Sector, StringType) :: StructField(
        colNames.GHSCode,
        StringType
      ) :: Nil
    )
    val array = Array[Any](null, null)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirBiologyActExtractor(SimpleExtractorCodes.empty).extractGroupId(input)

    // Then
    assert(result == DcirAct.groupID.Liberal)
  }

  it should "return Success(PrivateAmbulatory) if it is private ambulatory act" in {
    // Given
    val schema = StructType(
      StructField(colNames.Sector, StringType) :: StructField(
        colNames.GHSCode,
        DoubleType
      ) :: StructField(colNames.InstitutionCode, DoubleType) :: Nil
    )
    val array = Array[Any](null, 0D, 4D)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirBiologyActExtractor(SimpleExtractorCodes.empty).extractGroupId(input)

    // Then
    assert(result == DcirAct.groupID.PrivateAmbulatory)
  }

  it should "return Success(UnkownSource) if it is an act with unknown source" in {
    // Given
    val schema = StructType(
      StructField(colNames.Sector, StringType) ::
        StructField(colNames.GHSCode, DoubleType) ::
        StructField(colNames.InstitutionCode, DoubleType) ::
        Nil
    )
    val array = Array[Any](null, 1D, 4D)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirBiologyActExtractor(SimpleExtractorCodes.empty).extractGroupId(input)

    // Then
    assert(result == DcirAct.groupID.Unknown)
  }

  it should "return default value if the information of source of act is unavailable in DCIR" in {
    // Given
    val schema = StructType(
      StructField(colNames.GHSCode, DoubleType) ::
        StructField(colNames.Sector, StringType) :: Nil
    )
    val array = Array[Any](0D, 2D, 6D)
    val input = new GenericRowWithSchema(array, schema)
    // When
    val result = DcirBiologyActExtractor(SimpleExtractorCodes.empty).extractGroupId(input)

    // Then
    assert(result == DcirAct.groupID.DcirAct)
  }

  /*  "extract" should "return a Dataset of DCIR Biology Acts" in {

      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      // Given
      val codes = BaseExtractorCodes(List("AAAA", "CCCC"))

      val input = Seq(
        ("Patient_A", "AAAA", "CCAM1", Some(makeTS(2010, 1, 1)), None, None, None, makeTS(2010, 1, 1)),
        ("Patient_A", "BBBB", "CCAM1", Some(makeTS(2010, 2, 1)), Some(1D), Some(0D), Some(1D), makeTS(2010, 2, 1)),
        ("Patient_B", "CCCC", "CCAM1", Some(makeTS(2010, 3, 1)), None, None, None, makeTS(2010, 3, 1)),
        ("Patient_B", "CCCC", "CCAM1", Some(makeTS(2010, 4, 1)), Some(7D), Some(0D), Some(2D), makeTS(2010, 4, 1)),
        ("Patient_C", "BBBB", "CCAM1", None, Some(1D), Some(0D), Some(2D), makeTS(2010, 5, 1))
      ).toDF(
        colNames.PatientID, colNames.BioCode, colNames.CamCode, colNames.FlowDistributionDate,
        colNames.InstitutionCode, colNames.GHSCode, colNames.Sector, colNames.DcirFluxDate
      )

      val sources = Sources(dcir = Some(input))

      val expected = Seq[Event[MedicalAct]](
        BiologyDcirAct("Patient_A", BiologyDcirAct.groupID.Liberal, "AAAA", 1.0, makeTS(2010, 1, 1)),
        BiologyDcirAct("Patient_B", BiologyDcirAct.groupID.Liberal, "CCCC", 1.0, makeTS(2010, 3, 1)),
        BiologyDcirAct("Patient_B", BiologyDcirAct.groupID.PrivateAmbulatory, "CCCC", 1.0, makeTS(2010, 4, 1))
      ).toDS

      // When
      val result = DcirBiologyActExtractor(codes).extract(sources)

      // Then
      assertDSs(result, expected)
    }*/

  "extract" should "return a Dataset of DCIR Biology Acts from raw data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val codes = SimpleExtractorCodes(List("238"))

    val input = sqlCtx.read.parquet("src/test/resources/test-input/DCIR_all.parquet")

    val sources = Sources(dcir = Some(input))

    val expected = Seq[Event[MedicalAct]](
      BiologyDcirAct("Patient_01", BiologyDcirAct.groupID.Liberal, "238", 0.0, makeTS(2006, 1, 15))
    ).toDS

    // When
    val result = DcirBiologyActExtractor(codes).extract(sources)

    // Then
    assertDSs(result, expected)
  }
}
