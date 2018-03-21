package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.SharedContext

class McoSourceSanitizerSuite extends SharedContext {

  "filterSpecialHospitals" should "remove lines containing any of the specific hospital codes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = McoSourceImplicits.ETA_NUM.toString

    val input = Seq("1", "2", "42", "690784178", "910100023").toDF(colName)

    val expected = Seq("1", "2", "42").toDF(colName)

    // When
    val instance = new McoSourceImplicits(input)
    val result = instance.filterSpecialHospitals

    // Then
    assertDFs(result, expected)
  }

  "filterNonReimbursedStays" should "remove lines containing non reimbursed" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = McoSourceImplicits.GHS_NUM.toString

    val input = Seq("1111", "9999", "4222", "4444", "9999").toDF(colName)

    val expected = Seq("1111", "4222", "4444").toDF(colName)

    // When
    val instance = new McoSourceImplicits(input)
    val result = instance.filterNonReimbursedStays

    // Then
    assertDFs(result, expected)
  }

  "filterSharedHospitalStays" should "remove lines that indicates shared hospital stays" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      McoSourceImplicits.SEJ_TYP,
      McoSourceImplicits.GRG_GHM
    ).map(col => col.toString)

    val input = Seq(
      (None, "28XXXX"),
      (Some("A"), "15A94Z"),
      (Some("B"), "15A94Z"),
      (Some("A"), "28XXXX"),
      (Some("A"), "28Z14Z"),
      (Some("B"), "28XXXX"),
      (Some("B"), "28Z14Z")
    ).toDF(colNames: _*)


    val expected = Seq(
      (None, "28XXXX"),
      (Some("A"), "15A94Z"),
      (Some("A"), "28XXXX"),
      (Some("A"), "28Z14Z"),
      (Some("B"), "28XXXX")
    ).toDF(colNames: _*)

    // When
    val instance = new McoSourceImplicits(input)
    val result = instance.filterSharedHospitalStays

    // Then
    assertDFs(result, expected)
  }

  "filterIVG" should "remove lines that indicates IVG hospitalisation" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = McoSourceImplicits.GRG_GHM.toString

    val input = Seq("14Z08Z", "12ZZZZ", "18Z08Z").toDF(colName)

    val expected = Seq("12ZZZZ", "18Z08Z").toDF(colName)

    // When
    val instance = new McoSourceImplicits(input)
    val result = instance.filterIVG

    // Then
    assertDFs(result, expected)
  }

  "filterMcoCorruptedHospitalStays" should "remove lines that indicates fictional hospital stays in MCO" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      McoSourceImplicits.GRG_GHM,
      McoSourceImplicits.NIR_RET,
      McoSourceImplicits.SEJ_RET,
      McoSourceImplicits.FHO_RET,
      McoSourceImplicits.PMS_RET,
      McoSourceImplicits.DAT_RET
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
    val instance = new McoSourceImplicits(input)
    val result = instance.filterMcoCorruptedHospitalStays

    // Then
    assertDFs(result, expected)
  }

  "filterMcoCeCorruptedHospitalStays" should "remove lines that indicates fictional hospital stays in MCO_CE" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List(
      McoSourceImplicits.NIR_RET,
      McoSourceImplicits.NAI_RET,
      McoSourceImplicits.SEX_RET,
      McoSourceImplicits.IAS_RET,
      McoSourceImplicits.ENT_DAT_RET
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
    val instance = new McoSourceImplicits(input)
    val result = instance.filterMcoCeCorruptedHospitalStays

    // Then
    assertDFs(result, expected)
  }

}
