package fr.polytechnique.cmap.cnam.etl.events.imb

import fr.polytechnique.cmap.cnam.etl.ColumnNames

trait ImbSource extends ColumnNames {

  final object ColNames {
    val PatientID: ColName = "NUM_ENQ"
    val Encoding: ColName = "MED_NCL_IDT"
    val Code: ColName = "MED_MTF_COD"
    val Date: ColName = "IMB_ALD_DTD"
  }
}
