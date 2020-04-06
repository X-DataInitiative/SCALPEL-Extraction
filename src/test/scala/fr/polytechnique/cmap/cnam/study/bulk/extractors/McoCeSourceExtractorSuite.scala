// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class McoCeSourceExtractorSuite extends SharedContext {
  "extract" should "extract available Events and warns when it fails if the tables have not been flattened" in {
    val sqlCtx = sqlContext

    // Given
    val mcoce = spark.read.parquet("src/test/resources/test-input/MCO_CE.parquet")
    val source = new Sources(mcoCe = Some(mcoce))
    val path = "target/test/output"
    val mcoCeSource = new McoCeSourceExtractor(path, "overwrite")
    // When
    mcoCeSource.extract(source)
    // Then, make sure everything is running.
    assert(true)
  }
}
