package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

class McoMedicalActsSuite extends SharedContext {

  "extract" should "return a DataSet of McoCIM10Act" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val cim10Codes = Set("C670")
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val expected = Seq[Event[MedicalAct]](
      McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 29)),
      McoCIM10Act("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 8))
    ).toDS

    val input = Sources(mco = Some(mco))
    // When
    val result = McoCimMedicalActExtractor.extract(input, cim10Codes)

    // Then
    assertDSs(result, expected)
  }

  it should "return all available McoCIM10Act when codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val expected = Seq[Event[MedicalAct]](
      McoCIM10Act("Patient_02", "10000123_10000543_2006", "C671", makeTS(2005, 12, 24)),
      McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 29)),
      McoCIM10Act("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_20000345_2007", "C671", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 8)),
      McoCIM10Act("Patient_02", "10000123_30000852_2008", "C671", makeTS(2008, 3, 15))
    ).toDS

    val input = Sources(mco = Some(mco))
    // When
    val result = McoCimMedicalActExtractor.extract(input, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return a DataSet of McoCCAMActs" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ccamCodes = Set("AAAA123")
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val expected = Seq[Event[MedicalAct]](
      McoCCAMAct("Patient_02", "10000123_10000987_2006", "AAAA123", makeTS(2005, 12, 29)),
      McoCCAMAct("Patient_02", "10000123_20000123_2007", "AAAA123", makeTS(2007, 1, 29)),
      McoCCAMAct("Patient_02", "10000123_30000546_2008", "AAAA123", makeTS(2008, 3, 8))
    ).toDS

    val input = Sources(mco = Some(mco))
    // When
    val result = McoCcamActExtractor.extract(input, ccamCodes)

    // Then
    assertDSs(result, expected)
  }

  it should "return all available McoCCAMActs when Codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val expected = Seq[Event[MedicalAct]](
      McoCCAMAct("Patient_02", "10000123_10000987_2006", "AAAA123", makeTS(2005, 12, 29)),
      McoCCAMAct("Patient_02", "10000123_20000123_2007", "AAAA123", makeTS(2007, 1, 29)),
      McoCCAMAct("Patient_02", "10000123_30000546_2008", "AAAA123", makeTS(2008, 3, 8)),
      McoCCAMAct("Patient_02", "10000123_20000345_2007", "BBBB123", makeTS(2007, 1, 29)),
      McoCCAMAct("Patient_02", "10000123_10000543_2006", "BBBB123", makeTS(2005, 12, 24)),
      McoCCAMAct("Patient_02", "10000123_30000852_2008", "BBBB123", makeTS(2008, 3, 15))
    ).toDS

    val input = Sources(mco = Some(mco))
    // When
    val result = McoCcamActExtractor.extract(input, Set.empty)

    // Then
    assertDSs(result, expected)
  }

}
