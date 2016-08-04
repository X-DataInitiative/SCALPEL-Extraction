package fr.polytechnique.cmap.cnam.filtering

import fr.polytechnique.cmap.cnam.SharedContext
import org.apache.spark.sql.DataFrame

/**
  * @author Daniel de Paula
  */
class IrPhaExtractorSuite extends SharedContext {

  "ErPhaExtractor.extract" should "return a DataFrame with the correct schema" in {
    // Given
    val path: String = "src/test/resources/expected/IR_PHA_R.parquet"
    val expected: DataFrame = sqlContext.read.parquet(path)

    // When
    val result = new IrPhaExtractor(sqlContext).extract(path)

    // Then
    assert(result.schema == expected.schema)
  }

  it should "fail if the path is invalid" in {
    // Given
    val path: String = "src/test/resources/expected/invalid_path.parquet"

    // Then
    intercept[java.lang.AssertionError] {
      new IrPhaExtractor(sqlContext).extract(path).count
    }
  }
}
