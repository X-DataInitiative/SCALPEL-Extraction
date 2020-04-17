// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DateType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mco.McoSource
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class FallHospitalStayExtractorSuite extends SharedContext {
  val colNames = new McoSource {}.ColNames
  val newColNames = new McoSource {}.NewColumns

  "extractEnd" should "return the end date from  end date column" in {
    // Given
    val schema = StructType(
      StructField(colNames.EndDate, DateType) ::
        StructField(newColNames.EstimatedStayStart, DateType) :: Nil
    )
    val array = Array[Any](makeTS(2020, 1, 3), makeTS(2020, 1, 1))
    val input = new GenericRowWithSchema(array, schema)
    val expected = makeTS(2020, 1, 3)
    //When
    val result = new FallHospitalStayExtractor(SimpleExtractorCodes.empty).extractEnd(input)
    assert(result.get == expected)
  }

  it should "fall back on the start date when the end date column is null" in {
    // Given
    val schema = StructType(
      StructField(colNames.EndDate, DateType) ::
        StructField(newColNames.EstimatedStayStart, DateType) :: Nil
    )
    val array = Array[Any](null, makeTS(2020, 1, 1))
    val input = new GenericRowWithSchema(array, schema)
    val expected = makeTS(2020, 1, 1)
    //When
    val result = new FallHospitalStayExtractor(SimpleExtractorCodes.empty).extractEnd(input)
    assert(result.get == expected, true)
  }
}
