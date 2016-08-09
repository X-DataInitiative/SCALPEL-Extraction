package fr.polytechnique.cmap.cnam.filtering.utilities

import org.apache.spark.sql.{Column}
import org.apache.spark.sql.types.{LongType, TimestampType}

object TransformerHelper {

  def getMeanTimestampColumn(timestamp1: Column, timestamp2: Column): Column = {
    ((timestamp1.cast(LongType) + timestamp2.cast(LongType)) / 2).cast(TimestampType)
  }
}
