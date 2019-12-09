// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.Diagnosis
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{McoAssociatedDiagnosisExtractor, McoLinkedDiagnosisExtractor, McoMainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor
import fr.polytechnique.cmap.cnam.study.fall.fractures.Surgery

trait ClassifyWeight extends McoExtractor[Diagnosis] with Surgery {
  override def extractWeight(r: Row): Double = {
    if (!r.isNullAt(r.fieldIndex(ColNames.ExitMode)) && getExit(r).equals("9")) {
      4
    } else if (!r.isNullAt(r.fieldIndex(ColNames.CCAM)) && codes.contains(r.getAs[String](ColNames.CCAM))) {
      3
    } else {
      2
    }
  }
}

object MainDiagnosisFallExtractor extends McoMainDiagnosisExtractor with ClassifyWeight

object AssociatedDiagnosisFallExtractor extends McoAssociatedDiagnosisExtractor with ClassifyWeight

object LinkedDiagnosisFallExtractor extends McoLinkedDiagnosisExtractor with ClassifyWeight
