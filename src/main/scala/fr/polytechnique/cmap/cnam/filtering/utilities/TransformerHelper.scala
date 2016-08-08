package fr.polytechnique.cmap.cnam.filtering.utilities

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.functions._

/**
  * Helper methods for the Transformer classes
  *
  * @author Daniel de Paula
  */
object TransformerHelper {

  /**
    * Returns a Timestamp Column with the mean date of two Timestamp Columns
    *
    * @param timestamp1
    * @param timestamp2
    * @return A Timestamp Column with the mean date of the two input columns
    */
  def getMeanTimestampColumn(timestamp1: Column, timestamp2: Column): Column = {
    ( (timestamp1.cast(LongType) + timestamp2.cast(LongType) ) / 2).cast(TimestampType)
  }
}
