package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.ImbDiagnosis
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class ImbDiagnosesSuite extends SharedContext {

  "extract" should "extract diagnosis events from raw data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val imb = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R.parquet")
    val expected = Seq(ImbDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13), makeTS(2016, 3, 13))).toDS

    val sources = Sources(irImb = Some(imb))
    // When
    val output = ImbDiagnosisExtractor.extract(sources, Set("C67"))

    // Then
    assertDSs(expected, output)
  }

  it should "extract all diagnosis events from raw data when an Empty codes is passed" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val imb = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R.parquet")
    val expected = Seq(
      ImbDiagnosis("Patient_02", "E11", makeTS(2006, 1, 25), makeTS(2011, 1, 24)),
      ImbDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13), makeTS(2016, 3, 13)),
      ImbDiagnosis("Patient_02", "9999", makeTS(2006, 4, 25), makeTS(2016, 4, 25))
    ).toDS

    val sources = Sources(irImb = Some(imb))
    // When
    val output = ImbDiagnosisExtractor.extract(sources, Set.empty).orderBy($"start".asc)

    // Then
    assertDSs(expected, output)
  }

  it should "extract all diagnosis events from raw data when an Empty codes is passed even when ir_imb_f is null" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val imb = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R_null.parquet")
    val expected = Seq(
      ImbDiagnosis("Patient_02", "E11", makeTS(2006, 1, 25), makeTS(2011, 1, 24)),
      ImbDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13), makeTS(2016, 3, 13)),
      ImbDiagnosis("Patient_03", "C67", makeTS(2006, 3, 13), makeTS(2100, 1, 1)),
      ImbDiagnosis("Patient_02", "9999", makeTS(2006, 4, 25), makeTS(2016, 4, 25))
    ).toDS

    val sources = Sources(irImb = Some(imb))
    // When
    val output = ImbDiagnosisExtractor.extract(sources, Set.empty).orderBy($"start".asc)

    // Then
    assertDSs(expected, output)
  }

}
