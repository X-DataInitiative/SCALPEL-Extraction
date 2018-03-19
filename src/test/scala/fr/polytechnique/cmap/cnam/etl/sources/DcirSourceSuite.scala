package fr.polytechnique.cmap.cnam.etl.sources

import fr.polytechnique.cmap.cnam.SharedContext
import org.apache.spark.sql.DataFrame

class DcirSourceSuite extends SharedContext {

  "sanitize" should "return a DataFrame without lines where the value for the column BSE_PRS_NAT is 0" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = Seq(("A", 1), ("B", 0), ("C", 10), ("D", 0)).toDF("NUM_ENQ", "BSE_PRS_NAT")
    val expected = Seq(("A", 1), ("C", 10)).toDF("NUM_ENQ", "BSE_PRS_NAT")

    // When
    val result = DcirSource.sanitize(input)

    // Then
    assertDFs(result, expected)
 }
}
