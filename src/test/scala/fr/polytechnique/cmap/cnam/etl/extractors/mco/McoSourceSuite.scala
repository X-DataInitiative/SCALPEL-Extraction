// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.mco

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoSourceSuite extends SharedContext with McoSource {

  lazy val fakeMcoData = {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    Seq(
      ("HasCancer1", Some("C669"), Some("C672"), Some("C643"), Some(9), Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)), Some("01122011"), Some("12122011")),
      ("HasCancer1", Some("C679"), Some("C691"), Some("C643"), Some(0), Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)), Some("01122011"), Some("12122011")),
      ("HasCancer2", Some("C669"), Some("C672"), Some("C643"), Some(0), Some(12), Some(2011), 11,
        None, Some(makeTS(2011, 12, 12)), None, Some("12122011")),
      ("HasCancer3", Some("C669"), Some("C672"), Some("C643"), Some(0), Some(12), Some(2011), 11,
        None, None, None, None),
      ("HasCancer4", Some("C669"), Some("C672"), Some("C643"), Some(0), None, None, 11,
        None, Some(makeTS(2011, 12, 12)), None, Some("12122011")),
      ("HasCancer5", Some("C679"), Some("B672"), Some("C673"), Some(0), Some(1), Some(2010), 31,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)), Some("01122011"), Some("12122011")),
      ("MustBeDropped1", None, None, None, Some(0), Some(1), Some(2010), 31,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)), Some("01122011"), Some("12122011")),
      ("MustBeDropped2", None, Some("7"), None, Some(0), Some(1), Some(2010), 31,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)), Some("01122011"), Some("12122011"))
    ).toDF(
      "NUM_ENQ", "MCO_D__ASS_DGN", "MCO_B__DGN_PAL", "MCO_B__DGN_REL", "SOR_MOD", "MCO_B__SOR_MOI",
      "MCO_B__SOR_ANN", "MCO_B__SEJ_NBJ", "ENT_DAT", "SOR_DAT", "ENT_DAT_STR", "SOR_DAT_STR"
    )
  }

  "estimateStayStartTime" should "estimate a stay starting date using MCO available data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = fakeMcoData
      .withColumn("ENT_DAT", col("ENT_DAT_STR"))
      .withColumn("SOR_DAT", col("SOR_DAT_STR"))
    val expected: DataFrame = Seq(
      Tuple1(makeTS(2011, 12, 1)),
      Tuple1(makeTS(2011, 12, 1)),
      Tuple1(makeTS(2011, 12, 1)),
      Tuple1(makeTS(2011, 11, 20)),
      Tuple1(makeTS(2011, 12, 1)),
      Tuple1(makeTS(2011, 12, 1)),
      Tuple1(makeTS(2011, 12, 1)),
      Tuple1(makeTS(2011, 12, 1))
    ).toDF(NewColumns.EstimatedStayStart)


    // When
    val output: DataFrame = input
      .estimateStayStartTime
      .select(NewColumns.EstimatedStayStart)

    // Then
    assertDFs(output, expected)
  }
}
