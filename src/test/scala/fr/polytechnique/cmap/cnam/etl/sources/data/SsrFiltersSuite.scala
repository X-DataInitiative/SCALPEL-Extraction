package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class SsrFiltersSuite extends SharedContext {

  "filterSpecialHospitals" should "remove lines containing any of the specific hospital codes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = SsrSource.ETA_NUM.toString

    val input = Seq("1", "2", "42", "690784178", "910100023", "940019144").toDF(colName)

    val expected = Seq("1", "2", "42").toDF(colName)

    // When
    val instance = new SsrFilters(input)
    val result = instance.filterSpecialHospitals

    // Then
    assertDFs(result, expected)
  }

  "filterSsrCorruptedHospitalStays" should "remove lines that indicates fictional hospital stays in SSR" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      SsrSource.NIR_RET,
      SsrSource.SEJ_RET,
      SsrSource.FHO_RET,
      SsrSource.PMS_RET,
      SsrSource.DAT_RET,
      SsrSource.GRG_GME
    ).map(col => col.toString)

    val input = Seq(
      ("0", "0", "0", "0", "0", "2587"),
      ("1", "1", "1", "1", "1", "1111"),
      ("0", "0", "0", "0", "0", "1490"),
      ("1", "0", "0", "0", "0", "2547"),
      ("0", "0", "0", "0", "0", "9024")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("0", "0", "0", "0", "0", "2587"),
      ("0", "0", "0", "0", "0", "1490") //filtered
    ).toDF(colNames: _*)

    // When
    val instance = new SsrFilters(input)
    val result = instance.filterSsrCorruptedHospitalStays

    // Then
    assertDFs(result, expected)
  }

  "filterSsrCeCorruptedHospitalStays" should "remove lines that indicates fictional hospital stays in SSR_CE" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      SsrCeSource.NIR_RET,
      SsrCeSource.NAI_RET,
      SsrCeSource.SEX_RET,
      SsrCeSource.IAS_RET,
      SsrCeSource.ENT_DAT_RET
    ).map(col => col.toString)

    val input = Seq(
      ("0", "0", "0", "0", "0"),
      ("1", "1", "1", "1", "1"),
      ("1", "0", "0", "0", "0")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("0", "0", "0", "0", "0") //filtered
    ).toDF(colNames: _*)

    // When
    val instance = new SsrFilters(input)
    val result = instance.filterSsrCeCorruptedHospitalStays

    // Then
    assertDFs(result, expected)
  }
}
