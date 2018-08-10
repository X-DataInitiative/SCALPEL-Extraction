package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.McoCEAct
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoCEMedicalActsSuite extends SharedContext {

  "correctCamCode" should "return correct result for adequate row" in {
    import McoCEMedicalActs._

    // Given
    val codes = Seq("coloscopie")
    val schema = StructType(
      StructField(ColNames.PatientID, StringType) ::
        StructField(ColNames.CamCode, StringType) ::
        StructField(ColNames.Date, StringType) :: Nil)
    val data = Array[Any]("George", "coloscopie", "23012010")
    val input = new GenericRowWithSchema(data, schema)

    // When
    val result = McoCEMedicalActs.correctCamCode(codes)(input)

    // Then
    assert(result)
  }

  "extract" should "return acts that starts with the given codes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = List(
      ("george", "coloscopie", "01022003"),
      ("georgette", "angine", "01022003")
    ).toDF("NUM_ENQ", "MCO_FMSTC__CCAM_COD", "ENT_DAT")

    val expected = List(
      McoCEAct("georgette", "ACE", "angine", makeTS(2003, 2, 1))
    ).toDS

    // When
    val result = McoCEMedicalActs.extract(input, Seq("angi"))

    // Then
    assertDSs(expected, result)
  }



}
