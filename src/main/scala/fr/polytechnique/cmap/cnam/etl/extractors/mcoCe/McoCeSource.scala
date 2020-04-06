package fr.polytechnique.cmap.cnam.etl.extractors.mcoCe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.parseTimestamp

trait McoCeSource extends ColumnNames {

  final object ColNames extends Serializable {
    // Essential for all the Extractors
    val PatientID: ColName = "NUM_ENQ"
    val EtaNum: ColName = "ETA_NUM"
    val SeqNum : ColName = "SEQ_NUM"
    val Date = "EXE_SOI_DTD"
    val Year = "year"

    // For the Act extractor
    val CamCode = "MCO_FMSTC__CCAM_COD"

    // NGAP from FBSTC
    val NgapKeyLetterFbstc = "MCO_FBSTC__ACT_COD"
    val NgapCoefficientFbstc = "MCO_FBSTC__ACT_COE"

    // Practionner from FBSTC
    val PractitionnerSpecialtyFbstc = "MCO_FBSTC__EXE_SPE"

    // NGAP for FSCTC
    val NgapKeyLetterFcstc = "MCO_FCSTC__ACT_COD"
    val NgapCoefficientFcstc = "MCO_FCSTC__ACT_COE"

    // Practionner from FCSTC
    val PractitionnerSpecialtyFcstc = "MCO_FCSTC__EXE_SPE"

    val core = List(PatientID, EtaNum, SeqNum, Date, Year)

  }

}
