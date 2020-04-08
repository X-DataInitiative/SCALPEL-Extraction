// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.extractors.sources.ssrce.SsrCeSource
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class SsrCeSourceExtractorSuite extends SharedContext {
  "extract" should "extract available Events and warns when it fails if the tables have not been flattened" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val colNames = new SsrCeSource {}.ColNames
    // Given
    val ssrCe = Seq(
      ("Patient_A", "AAAA", makeTS(2010, 1, 1)),
      ("Patient_A", "BBBB", makeTS(2010, 2, 1)),
      ("Patient_B", "CCCC", makeTS(2010, 3, 1)),
      ("Patient_B", "CCCC", makeTS(2010, 4, 1)),
      ("Patient_C", "BBBB", makeTS(2010, 5, 1))
    ).toDF(
      colNames.PatientID, colNames.CamCode, colNames.StartDate
    )
    val source = new Sources(ssrCe = Some(ssrCe))
    val path = "target/test/output"
    val ssrCeSource = new SsrCeSourceExtractor(path, "overwrite")
    // When
    ssrCeSource.extract(source)
    // Then, make sure everything is running.
    assert(true)
  }
}
