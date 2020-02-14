package fr.polytechnique.cmap.cnam.etl.extractors.mcoCe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.parseTimestamp

trait McoCeSource extends ColumnNames {

  final object ColNames extends Serializable {
    val PatientID: ColName = "NUM_ENQ"
    val EtaNum: ColName = "ETA_NUM"
    val SeqNum : ColName = "SEQ_NUM"
    val CamCode = "MCO_FMSTC__CCAM_COD"
    val Date = "EXE_SOI_DTD"
    val NgapKeyLetterFbstc = "MCO_FBSTC__ACT_COD"
    val NgapCoefficientFbstc = "MCO_FBSTC__ACT_COE"
    val PractitionnerSpecialtyFbstc = "MCO_FBSTC__EXE_SPE"
    val NgapKeyLetterFcstc = "MCO_FCSTC__ACT_COD"
    val NgapCoefficientFcstc = "MCO_FCSTC__ACT_COE"
    val PractitionnerSpecialtyFcstc = "MCO_FCSTC__EXE_SPE"
    val Year = "year"

    val all = List(
      PatientID, EtaNum, SeqNum, Year, CamCode, Date,
      NgapKeyLetterFbstc, NgapCoefficientFbstc, PractitionnerSpecialtyFbstc,
      NgapKeyLetterFcstc, NgapCoefficientFcstc, PractitionnerSpecialtyFcstc
    )
  }

}
