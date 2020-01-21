package fr.polytechnique.cmap.cnam.etl.extractors.dcir

import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames

/** Trait to retrieve the columns of dcir dataframe. */
trait DcirSource extends ColumnNames {

  final object ColNames extends Serializable {
    lazy val PatientID: ColName = "NUM_ENQ"
    lazy val MSpe: ColName = "PSE_SPE_COD"
    lazy val NonMSpe: ColName = "PSE_ACT_NAT"
    lazy val ExecPSNum: ColName = "PFS_EXE_NUM"
    lazy val DcirEventStart: ColName = "EXE_SOI_DTD"
    lazy val DcirFluxDate: ColName = "FLX_DIS_DTD"
    lazy val CamCode: String = "ER_CAM_F__CAM_PRS_IDE"
    lazy val BioCode: String = "ER_BIO_F__BIO_PRS_IDE"
    lazy val GHSCode: String = "ER_ETE_F__ETE_GHS_NUM"
    lazy val InstitutionCode: String = "ER_ETE_F__ETE_TYP_COD"
    lazy val Sector: String = "ER_ETE_F__PRS_PPU_SEC"
    lazy val Date: String = "EXE_SOI_DTD"
    lazy val NaturePrestation: ColName = "PRS_NAT_REF"
    lazy val NgapCoefficient: ColName = "PRS_ACT_CFT"
    lazy val all = List(
      PatientID,
      CamCode,
      GHSCode,
      InstitutionCode,
      Sector,
      Date,
      MSpe,
      NonMSpe,
      ExecPSNum,
      DcirFluxDate,
      NaturePrestation,
      NgapCoefficient
    )


   lazy val DateStart: ColName = "FLX_DIS_DTD"
   lazy val DateEntry: ColName = "FLX_TRT_DTD"
   lazy val EmitterType: ColName = "FLX_EMT_TYP"
   lazy val EmitterId: ColName = "FLX_EMT_NUM"
   lazy val FlowNumber: ColName = "FLX_EMT_ORD"
   lazy val OrgId: ColName = "ORG_CLE_NUM"
   lazy val OrderId: ColName = "DCT_ORD_NUM"

  }

}
