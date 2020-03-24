// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, HospitalStay, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{SsrCcamActExtractor, SsrCsarrActExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.SsrHospitalStaysExtractor

class SsrSourceExtractor(override val path: String, override val saveMode: String) extends SourceExtractor(
  path,
  saveMode
) {
  override val sourceName: String = "SSR"
  override val extractors = List(
    ExtractorSources[MedicalAct](SsrCcamActExtractor, List("SSR_C", "SSR_CCAM"), "SSR_CCAM"),
    ExtractorSources[MedicalAct](SsrCsarrActExtractor, List("SSR_C", "SSR_CSARR"), "SSR_CSARR"),
    ExtractorSources[Diagnosis](SsrMainDiagnosisExtractor, List("SSR_C", "SSR_B"), "SSR_MAIN_DIAGNOSIS"),
    ExtractorSources[Diagnosis](SsrLinkedDiagnosisExtractor, List("SSR_C", "SSR_B"), "SSR_LINKED_DIAGNOSIS"),
    ExtractorSources[Diagnosis](SsrAssociatedDiagnosisExtractor, List("SSR_C", "SSR_D"), "SSR_ASSOCIATED_DIAGNOSIS"),
    ExtractorSources[Diagnosis](SsrTakingOverPurposeExtractor, List("SSR_C", "SSR_B"), "SSR_TAKE_OVER_REASON"),
    ExtractorSources[HospitalStay](SsrHospitalStaysExtractor, List("SSR_C", "SSR_B"), "SSR_STAY")
  )
}