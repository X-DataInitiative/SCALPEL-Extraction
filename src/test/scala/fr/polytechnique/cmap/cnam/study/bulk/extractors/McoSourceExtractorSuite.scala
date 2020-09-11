// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class McoSourceExtractorSuite extends SharedContext {
  "extract" should "extract available Events and warns when it fails if the tables have not been flattened" in {
    val sqlCtx = sqlContext

    // Given
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val source = new Sources(mco = Some(mco))
    val path = "target/test/output"
    val mcoSource = new McoSourceExtractor(path, "overwrite", "parquet")
    // When
    mcoSource.extract(source)
    // Then, make sure everything is running.
    assert(true)
  }
}
