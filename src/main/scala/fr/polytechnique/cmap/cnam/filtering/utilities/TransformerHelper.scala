package fr.polytechnique.cmap.cnam.filtering.utilities

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DoubleType, TimestampType}

object TransformerHelper {

  def getMeanTimestampColumn(timestamp1: Column, timestamp2: Column): Column = {
    ((timestamp1.cast(DoubleType) + timestamp2.cast(DoubleType)) / 2D).cast(TimestampType)
  }
}
