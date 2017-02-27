package fr.polytechnique.cmap.cnam.filtering.extraction

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.mockito.Mockito.mock
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.Sources
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames

class MoleculePurchasesSuite extends SharedContext {

  "extract" should "call the adequate private extractor" in {
    val sqlCtx = sqlContext

    // Given
    val config: ExtractionConfig = mock(classOf[ExtractionConfig])
    val dcir: DataFrame = sqlCtx.read.load("src/test/resources/test-input/DCIR.parquet")
    val irPha: DataFrame = sqlCtx.read.load("src/test/resources/test-input/IR_PHA_R.parquet")
    val dosages: DataFrame = sqlCtx.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/test-input/DOSE_PER_MOLECULE.CSV")
      .select(
        col("PHA_PRS_IDE"),
        col("MOLECULE_NAME"),
        col("TOTAL_MG_PER_UNIT")
      )
    val sources = new Sources(
      dcir = Some(dcir),
      irPha = Some(irPha),
      dosages = Some(dosages)
    )

    // Then
    import RichDataFrames._
    assert(
      MoleculePurchases.extract(config, sources).toDF ===
      DcirMoleculePurchases.extract(config, dcir, irPha, dosages).toDF
    )
  }
}