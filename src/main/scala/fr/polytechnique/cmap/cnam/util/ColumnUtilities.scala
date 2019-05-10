package fr.polytechnique.cmap.cnam.util

import java.sql.Timestamp
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}
import fr.polytechnique.cmap.cnam.util.functions._

object ColumnUtilities {

  def parseTimestamp(column: Column, pattern: String): Column = udf {
    v: String => functions.parseTimestamp(v, pattern)
  }.apply(column)

  def parseDate(column: Column, pattern: String): Column = udf {
    v: String => functions.parseDate(v, pattern)
  }.apply(column)

  def getMeanTimestampColumn(timestamp1: Column, timestamp2: Column): Column = {
    ((timestamp1.cast(DoubleType) + timestamp2.cast(DoubleType)) / 2D).cast(TimestampType)
  }

  def maxColumn(columns: Column*): Column = {
    if (columns.size == 1) {
      columns(0)
    } else {
      columns.reduce {
        (left: Column, right: Column) =>
          when(
            right.isNull, left
          ).otherwise(
            when(left >= right, left).otherwise(right)
          )
      }
    }
  }

  def minColumn(columns: Column*): Column = {
    if (columns.size == 1) {
      columns(0)
    } else {
      columns.reduce {
        (left: Column, right: Column) =>
          when(
            right.isNull, left
          ).otherwise(
            when(left <= right, left).otherwise(right)
          )
      }
    }
  }

  implicit class BucketizableTimestampColumn(column: Column) {

    // Discretizes the input column into time buckets.
    // For values before minTimestamp or after maxTimestamp, the resulting bucket will be null.
    def bucketize(minTimestamp: Timestamp, maxTimestamp: Timestamp, lengthDays: Int): Column = {

      val bucketCount: Int = (daysBetween(maxTimestamp, minTimestamp) / lengthDays).toInt
      val lastBucket = if (bucketCount > 0) bucketCount - 1 else 0

      val sanitized: Column = when(column.between(minTimestamp, maxTimestamp), column)

      val bucketId: Column = floor(datediff(sanitized, lit(minTimestamp)) / lengthDays).cast(IntegerType)
      when(bucketId <= lastBucket || bucketId.isNull, bucketId).otherwise(lastBucket)
    }
  }

}
