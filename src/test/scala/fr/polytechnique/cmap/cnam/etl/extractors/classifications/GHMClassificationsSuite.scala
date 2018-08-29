package fr.polytechnique.cmap.cnam.etl.extractors.classifications

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Classification, Event, GHMClassification}
import fr.polytechnique.cmap.cnam.util.functions._

class GHMClassificationsSuite extends SharedContext{

  "extract" should "return correct GHM" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val ghmCodes = Seq("12H50L")

    val expected = Seq(
      GHMClassification("Patient_02", "10000123_20000123_2007", "12H50L", makeTS(2007,1,29)),
      GHMClassification("Patient_02", "10000123_10000987_2006", "12H50L", makeTS(2005,12,29)),
      GHMClassification("Patient_02", "10000123_30000546_2008", "12H50L", makeTS(2008,3,8))
    ).toDS

    // When
    val result = GHMClassifications(ghmCodes).extract(input)

    // Then
    assertDSs(result, expected)

  }

  it should "return correct empty when given empty list" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val ghmCodes = Seq()

    val expected = sqlContext.sparkSession.emptyDataset[Event[Classification]]

    // When
    val result = GHMClassifications(ghmCodes).extract(input)

    // Then
    assertDSs(result, expected)

  }


}
