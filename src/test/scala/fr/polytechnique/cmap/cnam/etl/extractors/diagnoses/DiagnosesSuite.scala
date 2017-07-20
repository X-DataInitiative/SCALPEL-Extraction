package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class DiagnosesSuite extends SharedContext {

  "extract" should "call the adequate private extractors" in {

    // Given
    val config = DiagnosesConfig(
      mainDiagnosisCodes = List("C67", "C77", "C78", "C79"),
      linkedDiagnosisCodes = List("C67", "C77", "C78", "C79"),
      associatedDiagnosisCodes = List("C67"),
      imbDiagnosisCodes = List("C67")
    )
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
      new Diagnoses(config).extract(sources).toDF,
      unionDatasets(expectedMco, expectedImb).toDF
    )
  }
}
