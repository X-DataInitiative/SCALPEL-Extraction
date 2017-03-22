package fr.polytechnique.cmap.cnam.util

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions._

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
    assertDFs(result, expectedResult)
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
    assertDFs(result, expectedResult)
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
    assertDFs(result, expectedResult)
 }

  "minColumn" should "correctly return a column with the minimum value among a set of Numeric columns" in {
    val sqlCtx = this.sqlContext
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
    assertDFs(result, expectedResult)
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
    assertDFs(result, expectedResult)
 }

  "bucketize" should "bucketize (discretize) a timestamp column" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val minTimestamp = makeTS(2006, 1, 1)
    val maxTimestamp = makeTS(2007, 12, 31)
    val bucketSize = 30

    val input = Seq(
      Tuple1(Some(makeTS(2005, 12, 31))),
      Tuple1(Some(makeTS(2006, 1, 1))),
      Tuple1(Some(makeTS(2006, 6, 10))),
      Tuple1(Some(makeTS(2006, 12, 10))),
      Tuple1(Some(makeTS(2007, 4, 10))),
      Tuple1(Some(makeTS(2007, 8, 10))),
      Tuple1(Some(makeTS(2007, 12, 31))),
      Tuple1(Some(makeTS(2008, 1, 1))),
      Tuple1(None)
    ).toDF("input")

    val expected = Seq(
      (Some(makeTS(2005, 12, 31)), None),
      (Some(makeTS(2006, 1, 1)), Some(0)),
      (Some(makeTS(2006, 6, 10)), Some(5)),
      (Some(makeTS(2006, 12, 10)), Some(11)),
      (Some(makeTS(2007, 4, 10)), Some(15)),
      (Some(makeTS(2007, 8, 10)), Some(19)),
      (Some(makeTS(2007, 12, 31)), Some(23)),
      (Some(makeTS(2008, 1, 1)), None),
      (None, None)
    ).toDF("input", "output")

    // When
    import ColumnUtilities.BucketizableTimestampColumn
    val result = input.withColumn("output",
      col("input").bucketize(minTimestamp, maxTimestamp, bucketSize)
    )

    // Then
    assertDFs(result, expected)
  }
}
