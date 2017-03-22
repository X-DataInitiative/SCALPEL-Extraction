package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisException, DataFrame}
import fr.polytechnique.cmap.cnam.SharedContext

class DcirSuite extends SharedContext {

  "read" should "return a DataFrame with the correct schema" in {
    // Given
    val path: String = "src/test/resources/expected/DCIR.parquet"
    val expected: DataFrame = sqlContext.read.parquet(path)

    // When
    val result = Dcir.read(sqlContext, path)

    // Then
    assert(result.schema == expected.schema)
  }

  it should "return a DataFrame without lines where the value for the column BSE_PRS_NAT is 0" in {
    // Given
    val value = 0L
    val column = col("BSE_PRS_NAT")
    val path: String = "src/test/resources/expected/DCIR.parquet"

    // When
    val result = Dcir.read(sqlContext, path)

    // Then
    assert(result.filter(column === value).count == 0L)
 }

  it should "fail if the path is invalid" in {
    // Given
    val path: String = "src/test/resources/expected/invalid_path.parquet"

    // Then
    intercept[AnalysisException] {
      Dcir.read(sqlContext, path).count
    }
  }
}
