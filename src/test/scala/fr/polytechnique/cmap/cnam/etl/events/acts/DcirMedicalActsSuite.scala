package fr.polytechnique.cmap.cnam.etl.events.acts

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.util.functions._

class DcirMedicalActsSuite extends SharedContext {

  import DcirMedicalActs.ColNames

  val schema = StructType(
    StructField(ColNames.PatientID, StringType) ::
    StructField(ColNames.CamCode, StringType) ::
    StructField(ColNames.Date, DateType) :: Nil
  )

  "medicalActFromRow" should "return a Medical Act event when it's found in the row" in {

    // Given
    val codes = List("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "AAAA", makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)
    val expected = Some(DcirAct("Patient_A", DcirAct.groupID, "AAAA", makeTS(2010, 1, 1)))

    // When
    val result = DcirMedicalActs.medicalActFromRow(codes)(inputRow)

    // Then
    assert(result == expected)
  }

  it should "return None when no code is found in the row" in {

    // Given
    val codes = List("AAAA", "BBBB")
    val inputArray = Array[Any]("Patient_A", "CCCC", makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = DcirMedicalActs.medicalActFromRow(codes)(inputRow)

    // Then
    assert(result.isEmpty)
  }

  "extract" should "return a Dataset of Medical Acts" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val codes = List("AAAA", "CCCC")

    val input = Seq(
      ("Patient_A", "AAAA", makeTS(2010, 1, 1)),
      ("Patient_A", "BBBB", makeTS(2010, 2, 1)),
      ("Patient_B", "CCCC", makeTS(2010, 3, 1)),
      ("Patient_B", "CCCC", makeTS(2010, 4, 1)),
      ("Patient_C", "BBBB", makeTS(2010, 5, 1))
    ).toDF(ColNames.PatientID, ColNames.CamCode, ColNames.Date)

    val expected = Seq[Event[MedicalAct]](
      DcirAct("Patient_A", DcirAct.groupID, "AAAA", makeTS(2010, 1, 1)),
      DcirAct("Patient_B", DcirAct.groupID, "CCCC", makeTS(2010, 3, 1)),
      DcirAct("Patient_B", DcirAct.groupID, "CCCC", makeTS(2010, 4, 1))
    ).toDS

    // When
    val result = DcirMedicalActs.extract(input, codes)

    // Then
    assertDSs(result, expected)
  }
}
