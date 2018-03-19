package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.DataFrame

class DiagnosesSuite extends SharedContext {

  "extract" should "call the adequate private extractors" in {

    // Given
    val config = DiagnosesConfig(
      dpCodes = List("C67", "C77", "C78", "C79"),
      drCodes = List("C67", "C77", "C78", "C79"),
      daCodes = List("C67"),
      imbCodes = List("C67")
    )
    val mco: DataFrame = sqlContext.read.load("src/test/resources/test-input/MCO.parquet")
    val irImb: DataFrame = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R.parquet")
    val sources = new Sources(
      mco = Some(mco),
      irImb = Some(irImb)
    )
    val expectedMco = McoDiagnoses.extract(
      mco, config.dpCodes, config.drCodes, config.daCodes
    )
    val expectedImb = ImbDiagnoses.extract(irImb, config.imbCodes)

    // Then
    assertDFs(
      new Diagnoses(config).extract(sources).toDF,
      unionDatasets(expectedMco, expectedImb).toDF
    )
  }
}
