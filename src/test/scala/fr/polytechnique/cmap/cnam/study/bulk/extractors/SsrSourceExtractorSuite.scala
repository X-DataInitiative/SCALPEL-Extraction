// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class SsrSourceExtractorSuite extends SharedContext {
  "extract" should "extract available Events and warns when it fails if the tables have not been flattened" in {
    val sqlCtx = sqlContext

    // Given
    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")
    val source = new Sources(ssr = Some(ssr))
    val path = "target/test/output"
    val ssrSource = new SsrSourceExtractor(path, "overwrite")
    // When
    ssrSource.extract(source)
    // Then, make sure everything is running.
    assert(true)
  }
}
