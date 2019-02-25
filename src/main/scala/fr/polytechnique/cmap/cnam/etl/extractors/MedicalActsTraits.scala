package fr.polytechnique.cmap.cnam.etl.extractors.acts

import java.sql.Timestamp
import org.apache.spark.sql._
import fr.polytechnique.cmap.cnam._
import fr.polytechnique.cmap.cnam.etl.extractors.mco._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.parseTimestamp

import fr.polytechnique.cmap.cnam.etl.extractors.Columns

trait MedicalActsInfo {
  final lazy val PrivateInstitutionCodes = List(4, 5, 6, 7)
  final object PatientCols extends Columns {
    final val PatientID : String = "NUM_ENQ"
    final val CamCode : String = "ER_CAM_F__CAM_PRS_IDE"
    final val GHSCode : String = "ER_ETE_F__ETE_GHS_NUM"
    final val InstitutionCode : String = "ER_ETE_F__ETE_TYP_COD"
    final val Sector : String = "ER_ETE_F__PRS_PPU_SEC"
    final val Date : String = "EXE_SOI_DTD"
  }

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
    final val GHM : String = "MCO_B__GRG_GHM"
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
  final object ImbDiagnosesCols {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val Encoding = "MED_NCL_IDT"
    final lazy val Code = "MED_MTF_COD"
    final lazy val Date = "IMB_ALD_DTD"
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
