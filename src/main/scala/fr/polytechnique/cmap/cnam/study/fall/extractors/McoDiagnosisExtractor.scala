// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses.{McoAssociatedDiagnosisExtractor, McoLinkedDiagnosisExtractor, McoMainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mco.McoRowExtractor
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

class MainDiagnosisFallExtractor(codes: SimpleExtractorCodes) extends McoMainDiagnosisExtractor(codes) with ClassifyWeight

object MainDiagnosisFallExtractor {
  def apply(codes: SimpleExtractorCodes): MainDiagnosisFallExtractor = new MainDiagnosisFallExtractor(codes)
}

class AssociatedDiagnosisFallExtractor(codes: SimpleExtractorCodes) extends McoAssociatedDiagnosisExtractor(codes) with ClassifyWeight

object AssociatedDiagnosisFallExtractor {
  def apply(codes: SimpleExtractorCodes): AssociatedDiagnosisFallExtractor = new AssociatedDiagnosisFallExtractor(codes)
}

class LinkedDiagnosisFallExtractor(codes: SimpleExtractorCodes) extends McoLinkedDiagnosisExtractor(codes) with ClassifyWeight

object LinkedDiagnosisFallExtractor {
  def apply(codes: SimpleExtractorCodes): LinkedDiagnosisFallExtractor = new LinkedDiagnosisFallExtractor(codes)
}