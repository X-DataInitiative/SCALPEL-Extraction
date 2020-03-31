// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.ssrce

import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames

trait SsrCeSource extends ColumnNames {
  final object ColNames extends Serializable {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val StartDate = "EXE_SOI_DTD"
    final lazy val core = List(
      PatientID, StartDate
    )

    final lazy val CamCode = "SSR_FMSTC__CCAM_COD"
    final lazy val all = List(PatientID, CamCode, StartDate)
  }
}
