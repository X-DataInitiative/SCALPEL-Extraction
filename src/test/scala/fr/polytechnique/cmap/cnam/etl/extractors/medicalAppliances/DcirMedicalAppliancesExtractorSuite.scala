package fr.polytechnique.cmap.cnam.etl.extractors.medicalAppliances

import scala.util.Success

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{DcirMedicalAppliance, MedicalAppliance, DcirAct, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}
import org.scalatest.TryValues._


class DcirMedicalAppliancesExtractorSuite extends SharedContext {

  import DcirMedicalApplianceExtractor.ColNames

  val schema = StructType(
    StructField(ColNames.PatientID, StringType) ::
      StructField(ColNames.LppCode, StringType) ::
      StructField(ColNames.LppQuantity, StringType) ::
      StructField(ColNames.LppPrestationType, StringType) ::
      StructField(ColNames.LppDisposalEndDate, StringType) ::
      StructField(ColNames.InstitutionCode, DoubleType) ::
      StructField(ColNames.GHSCode, DoubleType) ::
      StructField(ColNames.Sector, DoubleType) ::
      StructField(ColNames.Date, DateType) :: Nil
  )

  "isInStudy" should "return true when a study code is found in the row" in {

    // Given
    val codes = Set("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "AAAA", null, null, null, makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = DcirMedicalApplianceExtractor.isInStudy(codes)(inputRow)

    // Then
    assert(result)
  }

  it should "return false when no code is found in the row" in {

    // Given
    val codes = Set("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "CCCC", 1D, 0D, 1D, makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = DcirMedicalApplianceExtractor.isInStudy(codes)(inputRow)

    // Then
    assert(!result)
  }

  "extract" should "return a Dataset with all DCIR Medical Appliances from raw data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = sqlCtx.read.parquet("src/test/resources/test-input/DCIR_ALL.parquet")

    val sources = Sources(dcir = Some(input))

    val expected = Seq[Event[MedicalAppliance]](
      DcirMedicalAppliance("Patient_01", "MjAwNi0wMi0wMV8yMDA2LTAxLTIwXzFfM18yX0NPREUxMjM0XzEw", "2117500", 1.0, makeTS(2006, 1, 15), makeTS(2006, 4, 1)),
      DcirMedicalAppliance("Patient_02", "MjAwNi0wMy0wMV8yMDA2LTAyLTA1XzNfNV80MTJfQ09ERTEyMzRfMTAwMDA=", "3122005", 1.0, makeTS(2006, 1, 15), makeTS(2006, 6, 1)),
      DcirMedicalAppliance("Patient_02", "MjAwNi0wMy0wMV8yMDA2LTAyLTA1XzNfNV80MTJfQ09ERTEyMzRfMTAwMDA=", "3121394", 1.0, makeTS(2006, 1, 15), makeTS(2006, 6, 1)),
      DcirMedicalAppliance("Patient_02", "MjAwNi0wMy0wMV8yMDA2LTAyLTA1XzNfNV80MTJfQ09ERTEyMzRfMTAwMDA=", "3112314", 1.0, makeTS(2006, 1, 15), makeTS(2006, 6, 1))
    ).toDS

    // When
    val result = DcirMedicalApplianceExtractor.extract(sources, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extractQueriedCodes" should "return a Dataset with queried codes DCIR Medical Appliances from raw data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val codes = Set("2117500", "3122005")
    val input = sqlCtx.read.parquet("src/test/resources/test-input/DCIR_ALL.parquet")

    val sources = Sources(dcir = Some(input))

    val expected = Seq[Event[MedicalAppliance]](
      DcirMedicalAppliance("Patient_01", "MjAwNi0wMi0wMV8yMDA2LTAxLTIwXzFfM18yX0NPREUxMjM0XzEw", "2117500", 1.0, makeTS(2006, 1, 15), makeTS(2006, 4, 1)),
      DcirMedicalAppliance("Patient_02", "MjAwNi0wMy0wMV8yMDA2LTAyLTA1XzNfNV80MTJfQ09ERTEyMzRfMTAwMDA=", "3122005", 1.0, makeTS(2006, 1, 15), makeTS(2006, 6, 1))
    ).toDS

    // When
    val result = DcirMedicalApplianceExtractor.extract(sources, codes)

    // Then
    assertDSs(result, expected)
  }
}

