// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class DcirSourceSuite extends SharedContext {

  "sanitize" should "return a DataFrame without lines where the value for the column BSE_PRS_NAT is 0" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = Seq(("A", 1, 0), ("B", 0, 71), ("C", 10, 3), ("D", 0, 4)).toDF("NUM_ENQ", "BSE_PRS_NAT", "DPN_QLF")
    val expected = Seq(("A", 1, 0), ("C", 10, 3)).toDF("NUM_ENQ", "BSE_PRS_NAT", "DPN_QLF")

    // When
    val result = DcirSource.sanitize(input)

    // Then
    assertDFs(result, expected)
  }
}
