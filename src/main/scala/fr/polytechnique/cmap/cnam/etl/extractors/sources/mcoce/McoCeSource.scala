package fr.polytechnique.cmap.cnam.etl.extractors.sources.mcoce

import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames

trait McoCeSource extends ColumnNames {

  final object ColNames extends Serializable {
    // Essential for all the Extractors
    val PatientID: ColName = "NUM_ENQ"
    val EtaNum: ColName = "ETA_NUM"
    val SeqNum: ColName = "SEQ_NUM"
    val CamCode = "MCO_FMSTC__CCAM_COD"
    val Year = "year"

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

    val StartDate: String = "EXE_SOI_DTD"
    val EndDate: String = "EXE_SOI_DTF"
    val ActCode: String = "MCO_FBSTC__ACT_COD"

    val core = List(
      PatientID, EtaNum, SeqNum, Year, StartDate
    )

    val all = List(
      PatientID, EtaNum, SeqNum, Year, CamCode, StartDate,
      NgapKeyLetterFbstc, NgapCoefficientFbstc, PractitionnerSpecialtyFbstc,
      NgapKeyLetterFcstc, NgapCoefficientFcstc, PractitionnerSpecialtyFcstc
    )


  }

}
