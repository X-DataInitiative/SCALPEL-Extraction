// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.imb

import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames

trait ImbSource extends ColumnNames{
  final object ColNames extends Serializable {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val Encoding = "MED_NCL_IDT"
    final lazy val Code = "MED_MTF_COD"
    final lazy val Date = "IMB_ALD_DTD"
    final lazy val EndDate = "IMB_ALD_DTF"
  }
}
