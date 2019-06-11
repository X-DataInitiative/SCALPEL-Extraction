package fr.polytechnique.cmap.cnam.etl.extractors

import fr.polytechnique.cmap.cnam.util.ColumnUtilities.parseTimestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}

trait MCOSourceInfo {

  final object MCOCols extends Columns {
    final val PatientID : String  = "NUM_ENQ"
    final val DP : String  = "MCO_B__DGN_PAL"
    final val DR : String = "MCO_B__DGN_REL"
    final val DA : String = "MCO_D__ASS_DGN"
    final val EtaNum : String  = "ETA_NUM"
    final val RsaNum : String  = "RSA_NUM"
    final val Year : String = "SOR_ANN"
    final val EstimatedStayStart : String  = "estimated_start"
    final val CCAM : String = "MCO_A__CDC_ACT"
  }

  final object MCOColsFull extends Columns  {
    final lazy val GHM : String = "MCO_B__GRG_GHM"
    final val StayEndMonth : String = "MCO_B__SOR_MOI"
    final val StayEndYear : String = "MCO_B__SOR_ANN"
    final val StayLength : String = "MCO_B__SEJ_NBJ"
    final val StayStartDate : String = "ENT_DAT"
    final val StayEndDate : String = "SOR_DAT"
  }
  final object MCOCECols extends Columns {
    final val PatientID : String = "NUM_ENQ"
    final val CamCode : String = "MCO_FMSTC__CCAM_COD"
    final val Date : String = "EXE_SOI_DTD"
  }


  implicit class StringToColumn(colName: String) {
    def toCol: Column = col(colName)
  }
  implicit class McoDataFrame(df: DataFrame) {
    final lazy val dayInMs = 24L * 60 * 60
    final lazy val timeDelta: Column = coalesce(MCOColsFull.StayLength.toCol, lit(0)) * dayInMs
    final lazy val estimate: Column = {
      val endDate = parseTimestamp(MCOColsFull.StayEndDate.toCol, "ddMMyyyy")
      (endDate.cast(LongType) - timeDelta).cast(TimestampType)
    }
    final lazy val roughEstimate: Column = (
      unix_timestamp(
        concat_ws("-", MCOColsFull.StayEndYear.toCol, MCOColsFull.StayEndMonth.toCol, lit("01 00:00:00"))
      ).cast(LongType) - timeDelta
    ).cast(TimestampType)
    final lazy val givenDate: Column = parseTimestamp(MCOColsFull.StayStartDate.toCol, "ddMMyyyy")
    /**
      * Estimate the stay starting date according to the different versions of PMSI MCO
      * Please note that in the case of early MCO (i.e. < 2009), the estimator is
      * date(01/month/year) - number of days of the stay.
      * This estimator is quite imprecise, and if one patient has several stays of the same
      * length in one month, it results in duplicate events.
      */
    def estimateStayStartTime: DataFrame = {
      df.withColumn(MCOCols.EstimatedStayStart, coalesce(givenDate, estimate, roughEstimate))
    }
  }
}
