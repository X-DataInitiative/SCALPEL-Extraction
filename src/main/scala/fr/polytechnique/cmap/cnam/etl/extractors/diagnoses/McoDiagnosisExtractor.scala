package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, StartsWithStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoBasicExtractor

protected trait BasicMcoDiagnosisExtractor extends McoBasicExtractor[Diagnosis] with StartsWithStrategy[Diagnosis]

case class McoMainDiagnosisExtractor(codes: BaseExtractorCodes) extends BasicMcoDiagnosisExtractor {
  override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = McoMainDiagnosis

  override def getCodes: BaseExtractorCodes = codes
}

case class McoAssociatedDiagnosisExtractor(codes: BaseExtractorCodes) extends BasicMcoDiagnosisExtractor {
  override val columnName: String = ColNames.DA
  override val eventBuilder: EventBuilder = McoAssociatedDiagnosis

  override def getCodes: BaseExtractorCodes = codes
}

case class McoLinkedDiagnosisExtractor(codes: BaseExtractorCodes) extends BasicMcoDiagnosisExtractor {
  override val columnName: String = ColNames.DR
  override val eventBuilder: EventBuilder = McoLinkedDiagnosis

  override def getCodes: BaseExtractorCodes = codes
}
