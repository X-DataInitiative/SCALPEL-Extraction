// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, HospitalStay, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.acts.{SsrCcamActExtractor, SsrCsarrActExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses._
import fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays.SsrHospitalStaysExtractor

class SsrSourceExtractor(override val path: String, override val saveMode: String, override val fileFormat: String)
  extends SourceExtractor(path, saveMode, fileFormat) {
  override val sourceName: String = "SSR"
  override val extractors = List(
    ExtractorSources[MedicalAct, SimpleExtractorCodes](
      SsrCcamActExtractor(SimpleExtractorCodes.empty),
      List("SSR_C", "SSR_CCAM"),
      "SSR_CCAM"
    ),
    ExtractorSources[MedicalAct, SimpleExtractorCodes](
      SsrCsarrActExtractor(SimpleExtractorCodes.empty),
      List("SSR_C", "SSR_CSARR"),
      "SSR_CSARR"
    ),
    ExtractorSources[Diagnosis, SimpleExtractorCodes](
      SsrMainDiagnosisExtractor(SimpleExtractorCodes.empty),
      List("SSR_C", "SSR_B"),
      "SSR_MAIN_DIAGNOSIS"
    ),
    ExtractorSources[Diagnosis, SimpleExtractorCodes](
      SsrLinkedDiagnosisExtractor(SimpleExtractorCodes.empty),
      List("SSR_C", "SSR_B"),
      "SSR_LINKED_DIAGNOSIS"
    ),
    ExtractorSources[Diagnosis, SimpleExtractorCodes](
      SsrAssociatedDiagnosisExtractor(SimpleExtractorCodes.empty),
      List("SSR_C", "SSR_D"),
      "SSR_ASSOCIATED_DIAGNOSIS"
    ),
    ExtractorSources[Diagnosis, SimpleExtractorCodes](
      SsrTakingOverPurposeExtractor(SimpleExtractorCodes.empty),
      List("SSR_C", "SSR_B"),
      "SSR_TAKE_OVER_REASON"
    ),
    ExtractorSources[HospitalStay, SimpleExtractorCodes](SsrHospitalStaysExtractor, List("SSR_C", "SSR_B"), "SSR_STAY")
  )
}