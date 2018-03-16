package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.{AnalysisException, DataFrame}
import fr.polytechnique.cmap.cnam.SharedContext

class SourceReaderSuite extends SharedContext {

  object TestSourceReader extends SourceManager

  "read" should "return the right dataframe" in {

    // Given
    val path: String = "src/test/resources/expected/IR_BEN_R.parquet"
    val expected: DataFrame = sqlContext.read.parquet("src/test/resources/expected/IR_BEN_R.parquet")


    // When
    val result = TestSourceReader.read(sqlContext, path)

    // Then
    assert(result.schema == expected.schema)
  }

  it should "fail is path is invalid" in {

    // Given
    val path: String = "src/test/resources/expected/invalid_path.parquet"

    // Then
    intercept[AnalysisException] {
      TestSourceReader.read(sqlContext, path).count
    }
  }
}
