package fr.polytechnique.cmap.cnam.etl.sources.value

import fr.polytechnique.cmap.cnam.SharedContext

class DosagesSourceSuite extends SharedContext {

  "read" should "return the correct DataFrame" in {
    // Given
    val path: String = "src/test/resources/value_tables/DOSE_PER_MOLECULE.CSV"
    val expectedCount = 632
    val expectedLine = "[2200789,METFORMINE,60000]"

    // When
    val result = DosagesSource.read(sqlContext, path)

    // Then
    assert(result.count() == expectedCount)
    assert(result.first().toString() == expectedLine)
  }

  "sanitize" should "return a DataFrame without the molecule BENFLUOREX" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val colName = DosagesSource.MoleculeName.toString
    val input = Seq("METFORMINE", "BENFLUOREX", "PIOGLITAZONE").toDF(colName)
    val expected = Seq("METFORMINE", "PIOGLITAZONE").toDF(colName)

    // When
    val result = DosagesSource.sanitize(input)

    // Then
    assertDFs(result, expected)
  }
}
