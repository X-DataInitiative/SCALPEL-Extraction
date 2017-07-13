package fr.polytechnique.cmap.cnam.etl.events.acts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.util.functions._

class McoMedicalActsSuite extends SharedContext {

  "extract" should "return a dataset with all found medical acts events" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val cim10Codes = List("C670", "C671")
    val ccamCodes = List("AAAA123")
    val input = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val expected = Seq[Event[MedicalAct]](
      McoCIM10Act("Patient_02", "10000123_10000543_2006", "C671", makeTS(2005, 12, 24)),
      McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 29)),
      McoCCAMAct("Patient_02", "10000123_10000987_2006", "AAAA123", makeTS(2005, 12, 29)),
      McoCIM10Act("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      McoCCAMAct("Patient_02", "10000123_20000123_2007", "AAAA123", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_20000345_2007", "C671", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 8)),
      McoCCAMAct("Patient_02", "10000123_30000546_2008", "AAAA123", makeTS(2008, 3, 8)),
      McoCIM10Act("Patient_02", "10000123_30000852_2008", "C671", makeTS(2008, 3, 15))
    ).toDS

    // When
    val result = McoMedicalActs.extract(input, cim10Codes, ccamCodes)

    // Then
    assertDSs(result, expected)
  }
}
