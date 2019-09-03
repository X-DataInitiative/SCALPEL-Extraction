package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor
import fr.polytechnique.cmap.cnam.study.fall.fractures.ChirurgicalOperation
import org.apache.spark.sql.Row

object MainDiagnosisExtractor extends McoExtractor[Diagnosis] with ChirurgicalOperation {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = MainDiagnosis

  override def extractWeight(r: Row): Double = {
    if (!r.isNullAt(r.fieldIndex(ColNames.Exit)) && getExit(r).equals("9"))
      4
    else if (!r.isNullAt(r.fieldIndex(ColNames.CCAM)) && operations.contains(r.getAs[String](ColNames.CCAM)))
      3
    else
      2
  }
}


object AssociatedDiagnosisExtractor extends McoExtractor[Diagnosis] with ChirurgicalOperation {
  final override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = AssociatedDiagnosis

  override def extractWeight(r: Row): Double = {
    if (!r.isNullAt(r.fieldIndex(ColNames.Exit)) && getExit(r).equals("9"))
      4
    else if (!r.isNullAt(r.fieldIndex(ColNames.CCAM)) && operations.contains(r.getAs[String](ColNames.CCAM)))
      3
    else
      2
  }
}


object LinkedDiagnosisExtractor extends McoExtractor[Diagnosis] {
  final override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = LinkedDiagnosis
}
