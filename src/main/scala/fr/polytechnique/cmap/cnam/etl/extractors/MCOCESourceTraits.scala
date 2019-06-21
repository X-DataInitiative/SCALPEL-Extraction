package fr.polytechnique.cmap.cnam.etl.extractors

trait MCOCESourceInfo {

  final object MCOCECols extends Columns {
    final val PatientID : String = "NUM_ENQ"
    final val CamCode : String = "MCO_FMSTC__CCAM_COD"
    final val Date : String = "EXE_SOI_DTD"
  }
}
