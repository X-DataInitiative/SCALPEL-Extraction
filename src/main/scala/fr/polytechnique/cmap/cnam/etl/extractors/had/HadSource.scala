package fr.polytechnique.cmap.cnam.etl.extractors.had

import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.parseTimestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

trait HadSource extends ColumnNames {

  final object ColNames extends Serializable {
    val PatientID: ColName = "NUM_ENQ"
    val EtaNumEpmsi: ColName = "ETA_NUM_EPMSI"
    val RhadNum: ColName = "RHAD_NUM"
    val DP: ColName = "HAD_B__DGN_PAL"
    val PEC_PAL: ColName = "HAD_B__PEC_PAL"
    val PEC_ASS: ColName = "HAD_B__PEC_ASS"
    val DA: ColName = "HAD_D__DGN_ASS"
    val CCAM: ColName = "HAD_A__CCAM_COD"

    // val GHM: ColName = "HAD_B__GRG_GHM" ?? TODO equivalent GHM pour la HAD

    val Year: ColName = "HAD_B__SOR_ANN"
    val StayStartDate: ColName = "ENT_DAT"
    val StayEndDate: ColName = "SOR_DAT"
    val StartDate: ColName = "EXE_SOI_DTD"
    val EndDate: ColName = "EXE_SOI_DTF"
    val all = List(
      PatientID, DP, DA, CCAM, PEC_PAL, PEC_ASS, EtaNumEpmsi, RhadNum, Year,
      StayStartDate, StayEndDate, StartDate, EndDate
    )
    val hospitalStayPart = List(
      PatientID, EtaNumEpmsi, RhadNum, Year, StartDate, EndDate
    )
  }

  implicit class HadDataFrame(df: DataFrame) {

    /**
      * Estimate the stay starting date according to the different versions of PMSI HAD
      * Please note that in the case of early HAD (i.e. < 2009), the estimator is
      * date(01/month/year) - number of days of the stay.
      * This estimator is quite imprecise, and if one patient has several stays of the same
      * length in one month, it results in duplicate events.
      */
    def estimateStayStartTime: DataFrame = {

      val estimate: Column = ColNames.StartDate.toCol

      val givenDate: Column = parseTimestamp(ColNames.StayStartDate.toCol, "ddMMyyyy")

      df.withColumn(
        NewColumns.EstimatedStayStart,
        coalesce(givenDate, estimate)
      )
    }
  }

  object NewColumns extends Serializable {
    val EstimatedStayStart: ColName = "estimated_start"
  }

}
