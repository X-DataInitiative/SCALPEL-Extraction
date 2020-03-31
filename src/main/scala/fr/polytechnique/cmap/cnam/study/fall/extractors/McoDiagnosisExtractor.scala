// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.BaseExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{McoAssociatedDiagnosisExtractor, McoLinkedDiagnosisExtractor, McoMainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoRowExtractor
import fr.polytechnique.cmap.cnam.study.fall.fractures.Surgery

trait ClassifyWeight extends Surgery {
  self : McoRowExtractor =>
  /** It gets ExitMode from row.
    *
    * @param r The row itself.
    * @return The value of ExitMode.
    */
  def getExit(r: Row): String = r.getAs[String](ColNames.ExitMode)

  override def extractWeight(r: Row): Double = {
    if (!r.isNullAt(r.fieldIndex(ColNames.ExitMode)) && getExit(r).equals("9")) {
      4
    } else if (!r.isNullAt(r.fieldIndex(ColNames.CCAM)) && surgeryCodes.contains(r.getAs[String](ColNames.CCAM))) {
      3
    } else {
      2
    }
  }
}

class MainDiagnosisFallExtractor(codes: BaseExtractorCodes) extends McoMainDiagnosisExtractor(codes) with ClassifyWeight

object MainDiagnosisFallExtractor {
  def apply(codes: BaseExtractorCodes): MainDiagnosisFallExtractor = new MainDiagnosisFallExtractor(codes)
}

class AssociatedDiagnosisFallExtractor(codes: BaseExtractorCodes) extends McoAssociatedDiagnosisExtractor(codes) with ClassifyWeight

object AssociatedDiagnosisFallExtractor {
  def apply(codes: BaseExtractorCodes): AssociatedDiagnosisFallExtractor = new AssociatedDiagnosisFallExtractor(codes)
}

class LinkedDiagnosisFallExtractor(codes: BaseExtractorCodes) extends McoLinkedDiagnosisExtractor(codes) with ClassifyWeight

object LinkedDiagnosisFallExtractor {
  def apply(codes: BaseExtractorCodes): LinkedDiagnosisFallExtractor = new LinkedDiagnosisFallExtractor(codes)
}