// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.ImbCcamDiagnosis
import fr.polytechnique.cmap.cnam.etl.extractors.BaseExtractorCodes
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class ImbDiagnosesSuite extends SharedContext {

  "extract" should "extract diagnosis events from raw data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val imb = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R.parquet")
    val expected = Seq(ImbCcamDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13), Some(makeTS(2016, 3, 13)))).toDS

    val sources = Sources(irImb = Some(imb))
    // When
    val output = ImbCimDiagnosisExtractor(BaseExtractorCodes(List("C67"))).extract(sources)

    // Then
    assertDSs(expected, output)
  }

  it should "extract all diagnosis events from raw data when an Empty codes is passed" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val imb = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R.parquet")
    val expected = Seq(
      ImbCcamDiagnosis("Patient_02", "E11", makeTS(2006, 1, 25), Some(makeTS(2011, 1, 24))),
      ImbCcamDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13), Some(makeTS(2016, 3, 13))),
      ImbCcamDiagnosis("Patient_02", "9999", makeTS(2006, 4, 25), Some(makeTS(2016, 4, 25)))
    ).toDS

    val sources = Sources(irImb = Some(imb))
    // When
    val output = ImbCimDiagnosisExtractor(BaseExtractorCodes.empty).extract(sources)

    // Then
    assertDSs(expected, output)
  }

  it should "extract all diagnosis events from raw data when an Empty codes is passed even when ir_imb_f is null" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    //val imb = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R_null.parquet")
    val imb = Seq(
      ("Patient_02", "CIM10", "E11", makeTS(2006, 1, 25), Some(makeTS(2011, 1, 24))),
      ("Patient_02", "CIM10", "C67", makeTS(2006, 3, 13), Some(makeTS(1600, 1, 1))),
      ("Patient_03", "CIM10", "C67", makeTS(2006, 3, 13), None),
      ("Patient_02", "CIM10", "9999", makeTS(2006, 4, 25), Some(makeTS(2016, 4, 25)))
    ).toDF("NUM_ENQ", "MED_NCL_IDT", "MED_MTF_COD", "IMB_ALD_DTD", "IMB_ALD_DTF")

    val expected = Seq(
      ImbCcamDiagnosis("Patient_02", "E11", makeTS(2006, 1, 25), Some(makeTS(2011, 1, 24))),
      ImbCcamDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13), None),
      ImbCcamDiagnosis("Patient_03", "C67", makeTS(2006, 3, 13), None),
      ImbCcamDiagnosis("Patient_02", "9999", makeTS(2006, 4, 25), Some(makeTS(2016, 4, 25)))
    ).toDS

    val sources = Sources(irImb = Some(imb))
    // When
    val output = ImbCimDiagnosisExtractor(BaseExtractorCodes.empty).extract(sources)

    // Then
    assertDSs(expected, output)
  }
}
