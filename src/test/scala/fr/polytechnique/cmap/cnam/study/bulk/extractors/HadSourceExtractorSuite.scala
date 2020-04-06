// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class HadSourceExtractorSuite extends SharedContext {
  "extract" should "extract available Events and warns when it fails if the tables have not been flattened" in {
    val sqlCtx = sqlContext

    // Given
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val source = new Sources(had = Some(had))
    val path = "target/test/output"
    val hadSource = new HadSourceExtractor(path, "overwrite")
    // When
    hadSource.extract(source)
    // Then, make sure everything is running.
    assert(true)
  }
}
