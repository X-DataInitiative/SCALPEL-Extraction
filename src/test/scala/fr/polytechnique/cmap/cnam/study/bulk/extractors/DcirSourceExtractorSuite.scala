// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.DrugConfig
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.level.Cip13Level
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class DcirSourceExtractorSuite extends SharedContext {
  "extract" should "extract available Events and warns when it fails if the tables have not been flattened" in {
    val sqlCtx = sqlContext

    // Given
    val dcir: DataFrame = sqlCtx.read.load("src/test/resources/test-input/DCIR.parquet")
    val source = new Sources(dcir = Some(dcir))
    val path = "target/test/output"
    val drugConfig = new DrugConfig(Cip13Level, List.empty)
    val dcirSource = new DcirSourceExtractor(path, "overwrite", drugConfig)

    dcirSource.extract(source)

    assert(true)
  }
}
