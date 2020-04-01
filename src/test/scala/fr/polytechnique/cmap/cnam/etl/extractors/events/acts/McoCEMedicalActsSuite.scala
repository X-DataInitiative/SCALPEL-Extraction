// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, McoCeCcamAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mcoce.McoCeSource
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoCEMedicalActsSuite extends SharedContext {

  "isInStudy" should "return true if row is in study" in {
    val colNames = new McoCeSource {}.ColNames
    // Given
    val codes = SimpleExtractorCodes(List("coloscopie"))
    val schema = StructType(
      StructField(colNames.PatientID, StringType) ::
        StructField(colNames.CamCode, StringType) ::
        StructField(colNames.Date, StringType) :: Nil
    )
    val data = Array[Any]("George", "coloscopie", "23012010")
    val input = new GenericRowWithSchema(data, schema)

    // When
    val result = McoCeCcamActExtractor(codes).isInStudy(input)

    // Then
    assert(result)
  }

  "extract" should "return acts that starts with the given codes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val cim10Codes = SimpleExtractorCodes(List("DEM"))
    val mcoCe = spark.read.parquet("src/test/resources/test-input/MCO_CE.parquet")
    val expected = Seq[Event[MedicalAct]](
      McoCeCcamAct("200410", "190000059_00022621_2014", "DEMP002", makeTS(2014, 4, 18))
    ).toDS

    val input = Sources(mcoCe = Some(mcoCe))
    // When
    val result = McoCeCcamActExtractor(cim10Codes).extract(input)

    // Then
    assertDSs(expected, result)
  }

  "extract" should "return all acts when codes are empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val mcoCe = spark.read.parquet("src/test/resources/test-input/MCO_CE.parquet")
    val expected = Seq[Event[MedicalAct]](
      McoCeCcamAct("200410", "190000059_00022621_2014", "DEMP002", makeTS(2014, 4, 18)),
      McoCeCcamAct("2004100010", "390780146_00098382_2014", "DZQM006", makeTS(2014, 11, 6)),
      McoCeCcamAct("2004100010", "390780146_00015211_2014", "DEQP005", makeTS(2014, 2, 11))
    ).toDS

    val input = Sources(mcoCe = Some(mcoCe))
    // When
    val result = McoCeCcamActExtractor(SimpleExtractorCodes.empty).extract(input)

    // Then
    assertDSs(expected, result)
  }
}
