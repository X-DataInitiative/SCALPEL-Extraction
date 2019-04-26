package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.DataFrame

class PrestationsSuite extends SharedContext {

  "extract" should "call the adequate private extractors" in {

    // Given
    val config = PrestationsConfig(
      medicalSpeCodes = List("42"),
      nonMedicalSpeCodes= List("42")
    )
    val dcir: DataFrame = sqlContext.read.load("src/test/resources/test-input/DCIR.parquet")
    val sources = new Sources(
      dcir = Some(dcir)
    )
    val expectedDcir = DcirPrestationsExtractor(
      config.medicalSpeCodes, config.nonMedicalSpeCodes
    ).extract(dcir)

    // Then
    assertDSs(
      new Prestations(config).extract(sources),
      unionDatasets(expectedDcir)
    )
  }
}
