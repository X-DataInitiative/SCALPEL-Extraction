// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class McoFiltersSuite extends SharedContext {

  "filterSpecialHospitals" should "remove lines containing any of the specific hospital codes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = McoSource.ETA_NUM.toString

    val input = Seq("1", "2", "42", "690784178", "910100023").toDF(colName)

    val expected = Seq("1", "2", "42").toDF(colName)

    // When
    val instance = new McoFilters(input)
    val result = instance.filterSpecialHospitals

    // Then
    assertDFs(result, expected)
  }

  "filterNonReimbursedStays" should "remove lines containing non reimbursed" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = McoSource.GHS_NUM.toString

    val input = Seq("1111", "9999", "4222", "4444", "9999").toDF(colName)

    val expected = Seq("1111", "4222", "4444").toDF(colName)

    // When
    val instance = new McoFilters(input)
    val result = instance.filterNonReimbursedStays

    // Then
    assertDFs(result, expected)
  }

  "filterSharedHospitalStays" should "remove lines that indicates shared hospital stays" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      McoSource.SEJ_TYP,
      McoSource.ENT_MOD,
      McoSource.SOR_MOD,
      McoSource.GRG_GHM
    ).map(col => col.toString)

    val input = Seq(
      (None, "2", "3", "28XXXX"),
      (Some("A"), "2", "3", "15A94Z"),
      (Some("B"), "1", "1", "15A94Z"),
      (Some("A"), "2", "3", "28XXXX"),
      (Some("A"), "2", "3", "28Z14Z"),
      (Some("B"), "1", "1", "28XXXX"),
      (Some("B"), "1", "1", "28Z14Z")
    ).toDF(colNames: _*)


    val expected = Seq(
      (None, "2", "3", "28XXXX"),
      (Some("A"), "2", "3", "15A94Z"),
      (Some("A"), "2", "3", "28XXXX"),
      (Some("A"), "2", "3", "28Z14Z"),
      (Some("B"), "1", "1", "28XXXX")
    ).toDF(colNames: _*)

    // When
    val instance = new McoFilters(input)
    val result = instance.filterSharedHospitalStays

    // Then
    assertDFs(result, expected)
  }

  "filterIVG" should "remove lines that indicates IVG hospitalisation" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = McoSource.GRG_GHM.toString

    val input = Seq("14Z08Z", "12ZZZZ", "18Z08Z").toDF(colName)

    val expected = Seq("12ZZZZ", "18Z08Z").toDF(colName)

    // When
    val instance = new McoFilters(input)
    val result = instance.filterIVG

    // Then
    assertDFs(result, expected)
  }

  "filterMcoCorruptedHospitalStays" should "remove lines that indicates fictional hospital stays in MCO" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      McoSource.GRG_GHM,
      McoSource.NIR_RET,
      McoSource.SEJ_RET,
      McoSource.FHO_RET,
      McoSource.PMS_RET,
      McoSource.DAT_RET
    ).map(col => col.toString)

    val input = Seq(
      ("90XXXX", "0", "0", "0", "0", "0"),
      ("27XXXX", "1", "1", "1", "1", "1"),
      ("76XXXX", "0", "0", "0", "0", "0"),
      ("28XXXX", "1", "0", "0", "0", "0")
    ).toDF(colNames: _*)


    val expected = Seq(
      ("76XXXX", "0", "0", "0", "0", "0") //filtered
    ).toDF(colNames: _*)

    // When
    val instance = new McoFilters(input)
    val result = instance.filterMcoCorruptedHospitalStays

    // Then
    assertDFs(result, expected)
  }

  "filterMcoCeCorruptedHospitalStays" should "remove lines that indicates fictional hospital stays in MCO_CE" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      McoCeSource.NIR_RET,
      McoCeSource.NAI_RET,
      McoCeSource.SEX_RET,
      McoCeSource.IAS_RET,
      McoCeSource.ENT_DAT_RET
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
    val instance = new McoFilters(input)
    val result = instance.filterMcoCeCorruptedHospitalStays

    // Then
    assertDFs(result, expected)
  }
}
