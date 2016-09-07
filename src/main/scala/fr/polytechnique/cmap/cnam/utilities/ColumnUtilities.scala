package fr.polytechnique.cmap.cnam.utilities

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, TimestampType}

object ColumnUtilities {

  def getMeanTimestampColumn(timestamp1: Column, timestamp2: Column): Column = {
    ((timestamp1.cast(DoubleType) + timestamp2.cast(DoubleType)) / 2D).cast(TimestampType)
  }

  def maxColumn(columns: Column*): Column = {
    if(columns.size == 1) columns(0)
    else columns.reduce {
      (left: Column, right: Column) => when(
        right.isNull, left
      ).otherwise(
        when(left >= right, left).otherwise(right)
      )
    }
  }

  def minColumn(columns: Column*): Column = {
    if(columns.size == 1) columns(0)
    else columns.reduce {
      (left: Column, right: Column) => when(
        right.isNull, left
      ).otherwise(
        when(left <= right, left).otherwise(right)
      )
    }
  }
}
