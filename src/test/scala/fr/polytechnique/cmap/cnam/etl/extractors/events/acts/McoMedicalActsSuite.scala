// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

class McoMedicalActsSuite extends SharedContext {

  "extract" should "return a DataSet of McoCCAMActs" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ccamCodes = SimpleExtractorCodes(List("AAAA123"))
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val expected = Seq[Event[MedicalAct]](
      McoCCAMAct("Patient_02", "10000123_10000987_2006", "AAAA123", makeTS(2005, 12, 31)),
      McoCCAMAct("Patient_02", "10000123_20000123_2007", "AAAA123", makeTS(2007, 1, 31)),
      McoCCAMAct("Patient_02", "10000123_30000546_2008", "AAAA123", makeTS(2008, 3, 10))
    ).toDS

    val input = Sources(mco = Some(mco))
    // When
    val result = McoCcamActExtractor(ccamCodes).extract(input)

    // Then
    assertDSs(result, expected)
  }

  it should "return all available McoCCAMActs when Codes is Empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val expected = Seq[Event[MedicalAct]](
      McoCCAMAct("Patient_02", "10000123_10000987_2006", "AAAA123", makeTS(2005, 12, 31)),
      McoCCAMAct("Patient_02", "10000123_20000123_2007", "AAAA123", makeTS(2007, 1, 31)),
      McoCCAMAct("Patient_02", "10000123_30000546_2008", "AAAA123", makeTS(2008, 3, 10)),
      McoCCAMAct("Patient_02", "10000123_20000345_2007", "BBBB123", makeTS(2007, 1, 31)),
      McoCCAMAct("Patient_02", "10000123_10000543_2006", "BBBB123", makeTS(2005, 12, 26)),
      McoCCAMAct("Patient_02", "10000123_30000852_2008", "BBBB123", makeTS(2008, 3, 17))
    ).toDS

    val input = Sources(mco = Some(mco))
    // When
    val result = McoCcamActExtractor(SimpleExtractorCodes.empty).extract(input)

    // Then
    assertDSs(result, expected)
  }

}
