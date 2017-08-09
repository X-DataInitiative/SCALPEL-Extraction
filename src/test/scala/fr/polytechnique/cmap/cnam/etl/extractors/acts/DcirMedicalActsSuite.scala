package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.util.functions._

class DcirMedicalActsSuite extends SharedContext {

  import DcirMedicalActs.ColNames

  val schema = StructType(
    StructField(ColNames.PatientID, StringType) ::
    StructField(ColNames.CamCode, StringType) ::
    StructField(ColNames.InstitutionCode, StringType) ::
    StructField(ColNames.GHSCode, StringType) ::
    StructField(ColNames.Date, DateType) :: Nil
  )

  "medicalActFromRow" should "return a Medical Act event when it's found in the row" in {

    // Given
    val codes = List("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "AAAA", 1D, 0D, makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)
    val expected = Some(DcirAct("Patient_A", DcirAct.groupID.PublicAmbulatory, "AAAA", makeTS(2010, 1, 1)))

    // When
    val result = DcirMedicalActs.medicalActFromRow(codes)(inputRow)

    // Then
    assert(result == expected)
  }

  it should "return None when no code is found in the row" in {

    // Given
    val codes = List("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "CCCC", 1D, 0D, makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = DcirMedicalActs.medicalActFromRow(codes)(inputRow)

    // Then
    assert(result.isEmpty)
  }

  "getGHS" should "return the value in the correct column" in {
    // Given
    val schema = StructType(StructField(ColNames.GHSCode, DoubleType)::Nil)
    val inputArray = Array[Any](3D)
    val input = new GenericRowWithSchema(inputArray, schema)
    val expected = 3D

    // When
    val result = DcirMedicalActs.getGHS(input)

    // Then
    assert(result == expected)

  }

  "getInstitutionCode" should "return the value in the correct column" in {
    // Given
    val schema = StructType(StructField(ColNames.InstitutionCode, DoubleType)::Nil)
    val inputArray = Array[Any](52D)
    val input = new GenericRowWithSchema(inputArray, schema)
    val expected = 52D

    // When
    val result = DcirMedicalActs.getInstitutionCode(input)

    // Then
    assert(result == expected)

  }

  "getStatus" should "return correct status of private ambulatory" in {
    // Given
    val schema = StructType(
      StructField(ColNames.GHSCode, DoubleType)::
        StructField(ColNames.InstitutionCode, DoubleType)::Nil
    )
    val array = Array[Any](0D, 6D)
    val input = new GenericRowWithSchema(array, schema)
    val expected = DcirAct.groupID.PrivateAmbulatory

    // When
    val result = DcirMedicalActs.getGroupId(input)

    // Then
    assert(result == expected)

  }

  it should "return correct status of public ambulatory" in {
    // Given
    val schema = StructType(
      StructField(ColNames.GHSCode, DoubleType)::
        StructField(ColNames.InstitutionCode, DoubleType)::Nil
    )
    val array = Array[Any](0D, 0D)
    val input = new GenericRowWithSchema(array, schema)
    val expected = DcirAct.groupID.PublicAmbulatory

    // When
    val result = DcirMedicalActs.getGroupId(input)

    // Then
    assert(result == expected)

  }

  it should "return correct status of private hospitalization" in {
    // Given
    val schema = StructType(
      StructField(ColNames.GHSCode, DoubleType)::
        StructField(ColNames.InstitutionCode, DoubleType)::Nil
    )
    val array = Array[Any](12D, 6D)
    val input = new GenericRowWithSchema(array, schema)
    val expected = DcirAct.groupID.PrivateHospital

    // When
    val result = DcirMedicalActs.getGroupId(input)

    // Then
    assert(result == expected)

  }

  "extract" should "return a Dataset of Medical Acts" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val codes = List("AAAA", "CCCC")

    val input = Seq(
      ("Patient_A", "AAAA", makeTS(2010, 1, 1), 1D, 0D),
      ("Patient_A", "BBBB", makeTS(2010, 2, 1), 1D, 0D),
      ("Patient_B", "CCCC", makeTS(2010, 3, 1), 1D, 0D),
      ("Patient_B", "CCCC", makeTS(2010, 4, 1), 1D, 0D),
      ("Patient_C", "BBBB", makeTS(2010, 5, 1), 1D, 0D)
    ).toDF(ColNames.PatientID, ColNames.CamCode, ColNames.Date,
      ColNames.InstitutionCode, ColNames.GHSCode)

    val expected = Seq[Event[MedicalAct]](
      DcirAct("Patient_A", DcirAct.groupID.PublicAmbulatory, "AAAA", makeTS(2010, 1, 1)),
      DcirAct("Patient_B", DcirAct.groupID.PublicAmbulatory, "CCCC", makeTS(2010, 3, 1)),
      DcirAct("Patient_B", DcirAct.groupID.PublicAmbulatory, "CCCC", makeTS(2010, 4, 1))
    ).toDS

    // When
    val result = DcirMedicalActs.extract(input, codes)

    // Then
    assertDSs(result, expected)
  }
}
