package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.SharedContext

class ColumnUtilitiesSuite extends SharedContext{

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

    val expectedResult: DataFrame = Seq(
      Tuple1(Timestamp.valueOf("2010-01-01 00:00:00")),
      Tuple1(Timestamp.valueOf("2010-06-17 00:00:00")),
      Tuple1(Timestamp.valueOf("2004-12-31 12:00:00")),
      Tuple1(Timestamp.valueOf("2004-12-31 12:00:00")),
      Tuple1(Timestamp.valueOf("2010-01-01 12:00:00"))
    ).toDF("ts")

    // When
    val meanTimestampCol: Column = ColumnUtilities.getMeanTimestampColumn(col("ts1"), col("ts2"))
    val result: DataFrame = givenDf.select(meanTimestampCol.as("ts"))

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  "maxColumn" should "correctly return a column with the maximum value among a set of Numeric columns" in {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = Seq(
      (Some(1), Some(1), Some(1)),
      (Some(1), Some(10), Some(1)),
      (None, Some(1), Some(10)),
      (Some(1), None, Some(10)),
      (None, None, None)
    ).toDF("c1", "c2", "c3")

    val expectedResult: DataFrame = Seq(
      Tuple1(Some(1)),
      Tuple1(Some(10)),
      Tuple1(Some(10)),
      Tuple1(Some(10)),
      Tuple1(None)
    ).toDF("c")

    // When
    val result = givenDf.select(ColumnUtilities.maxColumn(col("c1"), col("c2"), col("c3")).as("c"))

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  it should "correctly return a column with the maximum value among a set of Timestamp columns" in {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = Seq(
      (Some(Timestamp.valueOf("2000-01-01 00:00:00")), Some(Timestamp.valueOf("2000-01-01 00:00:00")),
        Some(Timestamp.valueOf("2000-01-01 00:00:00"))),
      (Some(Timestamp.valueOf("2000-01-01 00:00:00")), Some(Timestamp.valueOf("2010-01-01 00:00:00")),
        Some(Timestamp.valueOf("2000-01-01 00:00:00"))),
      (None, Some(Timestamp.valueOf("2000-01-01 00:00:00")),
        Some(Timestamp.valueOf("2010-01-01 00:00:00"))),
      (Some(Timestamp.valueOf("2000-01-01 00:00:00")), None,
        Some(Timestamp.valueOf("2010-01-01 00:00:00"))),
      (None, None, None)
    ).toDF("c1", "c2", "c3")

    val expectedResult: DataFrame = Seq(
      Tuple1(Some(Timestamp.valueOf("2000-01-01 00:00:00"))),
      Tuple1(Some(Timestamp.valueOf("2010-01-01 00:00:00"))),
      Tuple1(Some(Timestamp.valueOf("2010-01-01 00:00:00"))),
      Tuple1(Some(Timestamp.valueOf("2010-01-01 00:00:00"))),
      Tuple1(None)
    ).toDF("c")

    // When
    val result = givenDf.select(ColumnUtilities.maxColumn(col("c1"), col("c2"), col("c3")).as("c"))

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  "minColumn" should "correctly return a column with the minimum value among a set of Numeric columns" in {
    val sqlCtx = this
      .sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = Seq(
      (Some(1), Some(1), Some(1)),
      (Some(10), Some(1), Some(10)),
      (None, Some(1), Some(10)),
      (Some(1), None, Some(10)),
      (None, None, None)
    ).toDF("c1", "c2", "c3")

    val expectedResult: DataFrame = Seq(
      Tuple1(Some(1)),
      Tuple1(Some(1)),
      Tuple1(Some(1)),
      Tuple1(Some(1)),
      Tuple1(None)
    ).toDF("c")

    // When
    val result = givenDf.select(ColumnUtilities.minColumn(col("c1"), col("c2"), col("c3")).as("c"))

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  it should "correctly return a column with the minimum value among a set of Timestamp columns" in {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = Seq(
      (Some(Timestamp.valueOf("2000-01-01 00:00:00")), Some(Timestamp.valueOf("2000-01-01 00:00:00")),
        Some(Timestamp.valueOf("2000-01-01 00:00:00"))),
      (Some(Timestamp.valueOf("2000-01-01 00:00:00")), Some(Timestamp.valueOf("2010-01-01 00:00:00")),
        Some(Timestamp.valueOf("2000-01-01 00:00:00"))),
      (None, Some(Timestamp.valueOf("2000-01-01 00:00:00")),
        Some(Timestamp.valueOf("2010-01-01 00:00:00"))),
      (Some(Timestamp.valueOf("2000-01-01 00:00:00")), None,
        Some(Timestamp.valueOf("2010-01-01 00:00:00"))),
      (None, None, None)
    ).toDF("c1", "c2", "c3")

    val expectedResult: DataFrame = Seq(
      Tuple1(Some(Timestamp.valueOf("2000-01-01 00:00:00"))),
      Tuple1(Some(Timestamp.valueOf("2000-01-01 00:00:00"))),
      Tuple1(Some(Timestamp.valueOf("2000-01-01 00:00:00"))),
      Tuple1(Some(Timestamp.valueOf("2000-01-01 00:00:00"))),
      Tuple1(None)
    ).toDF("c")

    // When
    val result = givenDf.select(ColumnUtilities.minColumn(col("c1"), col("c2"), col("c3")).as("c"))

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }
}
