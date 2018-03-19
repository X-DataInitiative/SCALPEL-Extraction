package fr.polytechnique.cmap.cnam.etl.sources

import fr.polytechnique.cmap.cnam.SharedContext

class McoSourceSuite extends SharedContext {

  "sanitize" should "remove lines containing any of the specific hospital codes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = McoSource.hospitalCodeColumn.toString
    val input = Seq("1", "2", "42",
      McoSource.specialHospitalCodes(5), McoSource.specialHospitalCodes(42)
    ).toDF(colName)
    val expected = Seq("1", "2", "42").toDF(colName)

    // When
    val result = McoSource.sanitize(input)

    // Then
    assertDFs(result, expected)
  }
}
