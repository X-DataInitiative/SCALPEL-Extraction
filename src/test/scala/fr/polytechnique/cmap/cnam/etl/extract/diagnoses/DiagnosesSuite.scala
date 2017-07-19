package fr.polytechnique.cmap.cnam.etl.extract.diagnoses

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class DiagnosesSuite extends SharedContext {

  "extract" should "call the adequate private extractors" in {

    // Given
    val config = ExtractionConfig.init()
    val mco: DataFrame = sqlContext.read.load("src/test/resources/test-input/MCO.parquet")
    val irImb: DataFrame = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R.parquet")
    val sources = new Sources(
      pmsiMco = Some(mco),
      irImb = Some(irImb)
    )
    val expectedMco = McoDiagnoses.extract(
      mco, config.mainDiagnosisCodes, config.linkedDiagnosisCodes, config.associatedDiagnosisCodes
    )
    val expectedImb = ImbDiagnoses.extract(irImb, config.imbDiagnosisCodes)

    // Then
    assertDFs(
      Diagnoses.extract(config, sources).toDF,
      unionDatasets(expectedMco, expectedImb).toDF
    )
  }
}
