package fr.polytechnique.cmap.cnam.etl.extractors.ssr

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class SsrSourceSuite extends SharedContext with SsrSource {

  lazy val fakeSsrData = {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    Seq(
      ("Patient1", Some("C669"), Some("C672"), Some("C643"), Some("12122011"), Some(2011), Some(12)),
      ("Patient1", Some("C679"), Some("C691"), Some("C643"), Some("01122011"), Some(2011), Some(12)),
      ("Patient1", Some("C679"), Some("C691"), Some("C643"), Some("15012012"), Some(2012), Some(1)),
      ("Patient2", Some("C669"), Some("C672"), Some("C643"), None, Some(2011), Some(11)),
      ("Patient3", Some("C679"), Some("B672"), Some("C673"), None, Some(2011), Some(5)),
      ("MustBeDropped1", None, None, None, Some("31122011"), Some(2011), Some(12))
    ).toDF(
      "NUM_ENQ", "SSR_B__MOR_PRP", "SSR_B__ETL_AFF", "SSR_D__DGN_COD",
      "ENT_DAT", "ANN_LUN_1S", "MOI_LUN_1S"
    )
  }

  "estimateStayStartTime" should "estimate a stay starting date using SSr available data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = fakeSsrData
    val expected: DataFrame = Seq(
      Tuple1(makeTS(2011, 12, 12)),
      Tuple1(makeTS(2011, 12, 1)),
      Tuple1(makeTS(2012, 1, 15)),
      Tuple1(makeTS(2011, 11, 1)),
      Tuple1(makeTS(2011, 5, 1)),
      Tuple1(makeTS(2011, 12, 31))
    ).toDF(NewColumns.EstimatedStayStart)


    // When
    val output: DataFrame = input
      .estimateStayStartTime
      .select(NewColumns.EstimatedStayStart)

    // Then
    assertDFs(output, expected)
  }
}
