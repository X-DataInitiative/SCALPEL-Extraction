package fr.polytechnique.cmap.cnam.util

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

object McoUtils {

  trait McoTimeColumns {
    val stayStart: Column = col("ENT_DAT").cast("Timestamp")
    val stayEnd: Column = col("SOR_DAT").cast("Timestamp")
    val stayEndMonth: Column = col("`MCO_B.SOR_MOI`")
    val stayEndYear: Column = col("`MCO_B.SOR_ANN`")
    val stayLength: Column = col("`MCO_B.SEJ_NBJ`")

    def inputColumns: List[Column] = List(
      stayStart, stayEnd, stayEndMonth, stayEndYear, stayLength
    )
  }



  implicit class McoDataFrame(df: DataFrame) {

    object McoColumns extends McoTimeColumns

    /**
      * Estimate the stay starting date according to the different versions of PMSI MCO
      * Please note that in the case of early MCO (i.e. < 2009), the estimator is
      * date(01/month/year) - number of days of the stay.
      * This estimator is quite imprecise, and if one patient has several stays of the same
      * length in one month, it results in duplicate events.
      */
    def estimateStayStartTime: DataFrame = {
      val cols = McoColumns
      val dayInMs = 24L * 60 * 60
      val timeDelta: Column = coalesce(cols.stayLength, lit(0)) * dayInMs
      val estimate: Column = (cols.stayEnd.cast(LongType) - timeDelta).cast(TimestampType)
      val roughEstimate: Column = (
        unix_timestamp(
          concat_ws("-", cols.stayEndYear, cols.stayEndMonth, lit("01 00:00:00"))
        ).cast(LongType) - timeDelta
        ).cast(TimestampType)

      df.withColumn("eventDate", coalesce(cols.stayStart, estimate, roughEstimate))
    }
  }
}
