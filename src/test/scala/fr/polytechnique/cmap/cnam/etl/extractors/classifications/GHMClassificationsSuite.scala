package fr.polytechnique.cmap.cnam.etl.extractors.classifications

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.GHMClassification
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

class GHMClassificationsSuite extends SharedContext {

  "extract" should "return correct GHM" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val ghmCodes = Set("12H50L")

    val expected = Seq(
      GHMClassification("Patient_02", "10000123_20000123_2007", "12H50L", makeTS(2007, 1, 29)),
      GHMClassification("Patient_02", "10000123_10000987_2006", "12H50L", makeTS(2005, 12, 29)),
      GHMClassification("Patient_02", "10000123_30000546_2008", "12H50L", makeTS(2008, 3, 8))
    ).toDS
    val sources = Sources(mco = Some(mco))

    // When
    val result = GhmExtractor.extract(sources, ghmCodes)

    // Then
    assertDSs(result, expected)

  }

  it should "return all available classifications when given empty code set" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")

    val expected = Seq(
      GHMClassification("Patient_02", "10000123_20000123_2007", "12H50L", makeTS(2007, 1, 29)),
      GHMClassification("Patient_02", "10000123_20000345_2007", "13J60M", makeTS(2007, 1, 29)),
      GHMClassification("Patient_02", "10000123_10000987_2006", "12H50L", makeTS(2005, 12, 29)),
      GHMClassification("Patient_02", "10000123_10000543_2006", "13J60M", makeTS(2005, 12, 24)),
      GHMClassification("Patient_02", "10000123_30000852_2008", "13J60M", makeTS(2008, 3, 15)),
      GHMClassification("Patient_02", "10000123_30000546_2008", "12H50L", makeTS(2008, 3, 8))
    ).toDS
    val sources = Sources(mco = Some(mco))

    // When
    val result = GhmExtractor.extract(sources, Set.empty)

    // Then
    assertDSs(result, expected)

  }
}
