package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.utilities.TransformerHelper
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._


/**
  * @author Daniel de Paula
  */
class TransformerHelperSuite extends SharedContext{

  "getMeanDateColumn" should "correctly calculate a timestamp column with the mean between two timestamp columns" in {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = Seq(
      (Timestamp.valueOf("2010-01-01 00:00:00"), Timestamp.valueOf("2010-01-01 00:00:00")),
      (Timestamp.valueOf("2010-01-01 00:00:00"), Timestamp.valueOf("2010-12-01 00:00:00")),
      (Timestamp.valueOf("2000-01-01 00:00:00"), Timestamp.valueOf("2010-01-01 00:00:00")),
      (Timestamp.valueOf("2010-01-01 00:00:00"), Timestamp.valueOf("2000-01-01 00:00:00")),
      (Timestamp.valueOf("2010-01-01 00:00:00"), Timestamp.valueOf("2010-01-02 00:00:00"))
    ).toDF("ts1", "ts2")

    val expectedResult: Seq[Timestamp] = Seq(
      Timestamp.valueOf("2010-01-01 00:00:00"),
      Timestamp.valueOf("2010-06-17 01:00:00"),
      Timestamp.valueOf("2004-12-31 12:00:00"),
      Timestamp.valueOf("2004-12-31 12:00:00"),
      Timestamp.valueOf("2010-01-01 12:00:00")
    )

    // When
    val meanTimestampCol: Column = TransformerHelper.getMeanTimestampColumn(col("ts1"), col("ts2"))
    val result: DataFrame = givenDf.select(meanTimestampCol)

    // Then
    assert(result.collect.toSeq.map(_.getAs[Timestamp](0)) == expectedResult)
  }
}
