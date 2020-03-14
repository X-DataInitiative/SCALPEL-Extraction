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

  "filterSharedHospitalStays" should "remove lines that indicates shared hospital stays" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      HadSource.ENT_MOD,
      HadSource.SOR_MOD
    ).map(col => col.toString)

    val input = Seq(
      ("2", "3"),
      ("1", "3"),
      ("1", "1")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("2", "3"),
      ("1", "3")
    ).toDF(colNames: _*)

    // When
    val instance = new HadFilters(input)
    val result = instance.filterSharedHospitalStays

    // Then
    assertDFs(result, expected)
  }
}
