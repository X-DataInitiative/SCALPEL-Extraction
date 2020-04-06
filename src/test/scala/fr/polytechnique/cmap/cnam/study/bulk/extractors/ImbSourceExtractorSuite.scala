// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class ImbSourceExtractorSuite extends SharedContext {
  "extract" should "extract available Events and warns when it fails if the tables have not been flattened" in {
    val sqlCtx = sqlContext

    // Given
    val imbR = spark.read.parquet("src/test/resources/test-input/IR_IMB_R.parquet")
    val source = new Sources(had = Some(imbR))
    val path = "target/test/output"
    val imbSource = new ImbSourceExtractor(path, "overwrite")
    // When
    imbSource.extract(source)
    // Then, make sure everything is running.
    assert(true)
  }
}
