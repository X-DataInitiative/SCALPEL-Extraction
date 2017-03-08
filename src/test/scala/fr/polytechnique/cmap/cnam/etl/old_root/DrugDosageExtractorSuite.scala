package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext

class DrugDosageExtractorSuite extends SharedContext {

  "extract" should "return the correct DataFrame" in {
    // Given
    val path: String = "src/test/resources/value_tables/DOSE_PER_MOLECULE.CSV"
    val extractor: Extractor = new DrugDosageExtractor(sqlContext)

    val expectedCount = 632
    val expectedLine = "[2200789,METFORMINE,60000]"

    // When
    val result = extractor.extract(path)

    // Then
    assert(result.count() == expectedCount)
    assert(result.first().toString() == expectedLine)
  }

  it should "return a DataFrame without the molecule BENFLUOREX" in {
    // Given
    val path: String = "src/test/resources/value_tables/DOSE_PER_MOLECULE.CSV"
    val extractor: Extractor = new DrugDosageExtractor(sqlContext)

    val expectedCount = 0

    // When
    val result = extractor.extract(path).where(col("MOLECULE_NAME") === "BENFLUOREX")

    // Then
    assert(result.count() == expectedCount)
  }

}
