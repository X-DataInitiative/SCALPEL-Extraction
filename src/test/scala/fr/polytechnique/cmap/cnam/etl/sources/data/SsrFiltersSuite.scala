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
      SsrSource.DAT_RET
    ).map(col => col.toString)

    val input = Seq(
      ("0", "0", "0", "0", "0", "0", "0"),
      ("1", "1", "1", "1", "1", "1", "1"),
      ("0", "0", "0", "0", "0", "0", "0"),
      ("1", "0", "0", "0", "0", "0", "0")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("0", "0", "0", "0", "0", "0", "0") //filtered
    ).toDF(colNames: _*)

    // When
    val instance = new SsrFilters(input)
    val result = instance.filterSsrCorruptedHospitalStays

    // Then
    assertDFs(result, expected)
  }

//  "filterSsrCeCorruptedHospitalStays" should "remove lines that indicates fictional hospital stays in MCO_CE" in {
//    val sqlCtx = sqlContext
//    import sqlCtx.implicits._
//
//    // Given
//    val colNames = List(
//      SsrCeSource.NIR_RET,
//      SsrCeSource.NAI_RET,
//      SsrCeSource.SEX_RET,
//      SsrCeSource.IAS_RET,
//      SsrCeSource.ENT_DAT_RET
//    ).map(col => col.toString)
//
//    val input = Seq(
//      ("0", "0", "0", "0", "0"),
//      ("1", "1", "1", "1", "1"),
//      ("1", "0", "0", "0", "0")
//    ).toDF(colNames: _*)
//
//
//    val expected = Seq(
//      ("0", "0", "0", "0", "0") //filtered
//    ).toDF(colNames: _*)
//
//    // When
//    val instance = new SsrFilters(input)
//    val result = instance.filterSsrCeCorruptedHospitalStays
//
//    // Then
//    assertDFs(result, expected)
//  }
}
