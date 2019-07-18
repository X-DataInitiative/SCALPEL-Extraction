package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class DcirFiltersSuite extends SharedContext{
  "filterInformationFlux" should "remove lines used for information purpose" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = Seq(("A", 1, 0), ("B", 0, 71), ("C", 10, 3), ("D", 0, 4)).toDF("NUM_ENQ", "BSE_PRS_NAT", "DPN_QLF")
    val expected = Seq(("A", 1, 0), ("C", 10, 3), ("D", 0, 4)).toDF("NUM_ENQ", "BSE_PRS_NAT", "DPN_QLF")

    // When
    val dcirFilters = new DcirFilters(input)
    val result = dcirFilters.filterInformationFlux

    // Then
    assertDFs(expected, result)
  }
}
