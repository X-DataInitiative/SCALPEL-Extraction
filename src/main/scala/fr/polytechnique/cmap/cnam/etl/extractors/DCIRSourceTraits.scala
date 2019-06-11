package fr.polytechnique.cmap.cnam.etl.extractors

trait DCIRSourceInfo {
  final lazy val PrivateInstitutionCodes = List(4, 5, 6, 7)
  final object PatientCols extends Columns {
    final val PatientID : String = "NUM_ENQ"
    final val CamCode : String = "ER_CAM_F__CAM_PRS_IDE"
    final val GHSCode : String = "ER_ETE_F__ETE_GHS_NUM"
    final val InstitutionCode : String = "ER_ETE_F__ETE_TYP_COD"
    final val Sector : String = "ER_ETE_F__PRS_PPU_SEC"
    final val Date : String = "EXE_SOI_DTD"
  }
}
