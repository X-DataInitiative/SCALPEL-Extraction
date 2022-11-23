package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class SsrFiltersSuite extends SharedContext {

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
      ("0", "0", "0", "0", "0", "900000"),
      ("1", "1", "1", "1", "1", "600000"),
      ("0", "0", "0", "0", "0", "800000"),
      ("1", "0", "0", "0", "0", "900000")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("0", "0", "0", "0", "0", "800000")
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

  "filterSharedHospitalStays" should "remove lines that indicates shared hospital stays" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      SsrSource.ENT_MOD,
      SsrSource.SOR_MOD,
      SsrSource.GRG_GME
    ).map(col => col.toString)

    val input = Seq(
      ("2", "3", "28XXXX"),
      ("2", "3", "15A94Z"),
      ("1", "1", "15A94Z"),
      ("2", "3", "28XXXX"),
      ("2", "3", "28Z14Z"),
      ("1", "1", "28XXXX"),
      ("1", "1", "28Z14Z")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("2", "3", "28XXXX"),
      ("2", "3", "15A94Z"),
      ("2", "3", "28XXXX"),
      ("2", "3", "28Z14Z"),
      ("1", "1", "28XXXX")
    ).toDF(colNames: _*)

    // When
    val instance = new SsrFilters(input)
    val result = instance.filterSharedHospitalStays

    // Then
    assertDFs(result, expected)
  }
}
