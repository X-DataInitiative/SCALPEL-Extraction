package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class HadFiltersSuite extends SharedContext {

  "filterHadCorruptedHospitalStays" should "remove lines that indicates fictional hospital stays in HAD" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      HadSource.NIR_RET,
      HadSource.SEJ_RET,
      HadSource.FHO_RET,
      HadSource.PMS_RET,
      HadSource.DAT_RET
    ).map(col => col.toString)

    val input = Seq(
      ("0", "0", "0", "0", "0"),
      ("1", "1", "1", "1", "1"),
      ("0", "0", "0", "0", "0"),
      ("1", "0", "0", "0", "0")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("0", "0", "0", "0", "0"),
      ("0", "0", "0", "0", "0") //filtered
    ).toDF(colNames: _*)

    // When
    val instance = new HadFilters(input)
    val result = instance.filterHadCorruptedHospitalStays

    // Then
    assertDFs(result, expected)
  }

  "filterSpecialHospitals" should "remove lines containing any of the specific hospital codes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = HadSource.ETA_NUM_EPMSI.toString

    val input = Seq("1", "2", "42", "690784178", "910100023", "940019144").toDF(colName)

    val expected = Seq("1", "2", "42").toDF(colName)

    // When
    val instance = new HadFilters(input)
    val result = instance.filterSpecialHospitals

    // Then
    assertDFs(result, expected)
  }

}
