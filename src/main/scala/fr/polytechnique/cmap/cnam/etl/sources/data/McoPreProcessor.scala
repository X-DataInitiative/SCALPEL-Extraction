package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.functions.{coalesce, concat_ws, lit, unix_timestamp}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoSource
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.parseTimestamp

object McoPreProcessor extends McoSource{

  implicit class McoDataFrame(df: DataFrame) {

    /**
      * Estimate the stay starting date according to the different versions of PMSI MCO
      * Please note that in the case of early MCO (i.e. < 2009), the estimator is
      * date(01/month/year) - number of days of the stay.
      * This estimator is quite imprecise, and if one patient has several stays of the same
      * length in one month, it results in duplicate events.
      */
    def estimateStayStartTime: DataFrame = {
      val dayInMs = 24L * 60 * 60
      val timeDelta: Column = coalesce(ColNames.StayLength.toCol, lit(0)) * dayInMs
      val estimate: Column = {
        val endDate = parseTimestamp(ColNames.StayEndDate.toCol, "ddMMyyyy")
        (endDate.cast(LongType) - timeDelta).cast(TimestampType)
      }
      val roughEstimate: Column = (
        unix_timestamp(
          concat_ws("-", ColNames.StayEndYear.toCol, ColNames.StayEndMonth.toCol, lit("01 00:00:00"))
        ).cast(LongType) - timeDelta
        ).cast(TimestampType)

      val givenDate: Column = parseTimestamp(ColNames.StayStartDate.toCol, "ddMMyyyy")

      df.withColumn(
        NewColumns.EstimatedStayStart,
        coalesce(givenDate, estimate, roughEstimate)
      )
    }
  }

  def prepareDF(df: DataFrame): DataFrame = df.estimateStayStartTime
}
