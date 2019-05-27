package fr.polytechnique.cmap.cnam.etl.extractors

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

trait IMBSourceInfo {

  final object ImbDiagnosesCols {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val Encoding = "MED_NCL_IDT"
    final lazy val Code = "MED_MTF_COD"
    final lazy val Date = "IMB_ALD_DTD"
  }

  implicit class StringToColumn(colName: String) {
    def toCol: Column = col(colName)
  }
}
