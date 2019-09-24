package fr.polytechnique.cmap.cnam.etl.extractors.had

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class HadSourceSuite extends SharedContext with HadSource {

  lazy val fakeHadData = {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    Seq(
      ("HasCancer1", Some("C669"), Some("C672"), Some("C643"),
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)), Some("01122011"), Some("12122011")),
      ("HasCancer1", Some("C679"), Some("C691"), Some("C643"),
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)), Some("01122011"), Some("12122011")),
      ("HasCancer2", Some("C669"), Some("C672"), Some("C643"),
        Some(makeTS(2011, 12, 11)), Some(makeTS(2011, 12, 12)), None, Some("12122011")),
      ("HasCancer4", Some("C669"), Some("C672"), Some("C643"),
        Some(makeTS(2011, 11, 11)), Some(makeTS(2011, 12, 12)), Some("11112011"), Some("12122011")),
      ("HasCancer5", Some("C679"), Some("B672"), Some("C673"),
        Some(makeTS(2010, 12, 1)), Some(makeTS(2010, 12, 12)), Some("01122010"), Some("12122010"))
    ).toDF(
      "NUM_ENQ", "HAD_D__ASS_DGN", "HAD_B__DGN_PAL", "HAD_B__PEC_PAL",
      "EXE_SOI_DTD", "EXE_SOI_DTF", "ENT_DAT_STR", "SOR_DAT_STR"
    )
  }

  "estimateStayStartTime" should "estimate a stay starting date using HAD available data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = fakeHadData
      .withColumn("ENT_DAT", col("ENT_DAT_STR"))
      .withColumn("SOR_DAT", col("SOR_DAT_STR"))
    val expected: DataFrame = Seq(
      (makeTS(2011, 12, 1), 2011),
      (makeTS(2011, 12, 1), 2011),
      (makeTS(2011, 12, 11), 2011),
      (makeTS(2011, 11, 11), 2011),
      (makeTS(2010, 12, 1), 2010)
    ).toDF(NewColumns.EstimatedStayStart, NewColumns.Year)


    // When
    val output: DataFrame = input
      .estimateStayStartTime
      .select(NewColumns.EstimatedStayStart, NewColumns.Year)

    // Then
    assertDFs(output, expected)
  }
}
