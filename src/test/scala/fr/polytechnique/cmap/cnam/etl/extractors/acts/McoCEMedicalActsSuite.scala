// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.acts

import java.sql.Date
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.McoCEAct
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoCEMedicalActsSuite extends SharedContext {

  "isInStudy" should "return true if row is in study" in {
    import McoCeActExtractor.ColNames
    // Given
    val codes = Set("coloscopie")
    val schema = StructType(
      StructField(ColNames.PatientID, StringType) ::
        StructField(ColNames.CamCode, StringType) ::
        StructField(ColNames.Date, StringType) :: Nil
    )
    val data = Array[Any]("George", "coloscopie", "23012010")
    val input = new GenericRowWithSchema(data, schema)

    // When
    val result = McoCeActExtractor.isInStudy(codes)(input)

    // Then
    assert(result)
  }

  "extract" should "return acts that starts with the given codes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val date = new Date(makeTS(2003, 2, 1).getTime)

    val input = List(
      ("george", "coloscopie", date),
      ("georgette", "angine", date)
    ).toDF("NUM_ENQ", "MCO_FMSTC__CCAM_COD", "EXE_SOI_DTD")

    val sources = Sources(mcoCe = Some(input))

    val expected = List(
      McoCEAct("georgette", "ACE", "angine", 1.0, makeTS(2003, 2, 1))
    ).toDS

    // When
    val result = McoCeActExtractor.extract(sources, Set("angi"))

    // Then
    assertDSs(expected, result)
  }

  "extract" should "return all acts when codes are empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val date = new Date(makeTS(2003, 2, 1).getTime)

    val input = List(
      ("george", "coloscopie", date),
      ("georgette", "angine", date)
    ).toDF("NUM_ENQ", "MCO_FMSTC__CCAM_COD", "EXE_SOI_DTD")

    val sources = Sources(mcoCe = Some(input))

    val expected = List(
      McoCEAct("georgette", "ACE", "angine", 1.0, makeTS(2003, 2, 1)),
      McoCEAct("george", "ACE", "coloscopie", 1.0, makeTS(2003, 2, 1))
    ).toDS

    // When
    val result = McoCeActExtractor.extract(sources, Set.empty)

    // Then
    assertDSs(expected, result)
  }
}
