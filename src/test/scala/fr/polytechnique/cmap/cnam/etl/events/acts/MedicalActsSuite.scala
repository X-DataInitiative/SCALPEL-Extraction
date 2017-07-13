package fr.polytechnique.cmap.cnam.etl.events.acts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MedicalActsSuite extends SharedContext {

  "extract" should "find all medical acts in all sources" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExtractionConfig.init().copy(
      dcirMedicalActCodes = List("ABCD123"),
      mcoCIM10MedicalActCodes = List("C670", "C671"),
      mcoCCAMMedicalActCodes = List("AAAA123")
    )
    val sources = {
      val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
      val dcir = spark.read.parquet("src/test/resources/test-input/DCIR.parquet")
      Sources(pmsiMco = Some(mco), dcir = Some(dcir))
    }
    val expected = Seq[Event[MedicalAct]](
      // DcirAct("Patient_01", "dcir", "ABCD123", null), // The dummy data contains a null value
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
    val result = MedicalActs.extract(config, sources)

    // Then
    sources.dcir.get.show
    assertDSs(result, expected)
  }
}
