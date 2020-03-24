// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.acts.McoCcamActExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{McoAssociatedDiagnosisExtractor, McoLinkedDiagnosisExtractor, McoMainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.McoHospitalStaysExtractor

class McoSourceExtractor(override val path: String, override val saveMode: String) extends SourceExtractor(
  path,
  saveMode
) {
  override val sourceName: String = "MCO"
  override val extractors = List(
    ExtractorSources[MedicalAct](McoCcamActExtractor, List("MCO_C", "MCO_A"), "MCO_CCAM_ACT"),
    ExtractorSources[Diagnosis](McoMainDiagnosisExtractor, List("MCO_C", "MCO_B"), "MCO_MAIN_DIAGNOSIS"),
    ExtractorSources[Diagnosis](
      McoAssociatedDiagnosisExtractor,
      List("MCO_C", "MCO_B", "MCO_D"),
      "MCO_ASSOCIATED_DIAGNOSIS"
    ),
    ExtractorSources[Diagnosis](McoLinkedDiagnosisExtractor, List("MCO_C", "MCO_B"), "MCO_LINKED_DIAGNOSIS"),
    ExtractorSources[HospitalStay](McoHospitalStaysExtractor, List("MCO_C", "MCO_B"), "MCO_HOSPITAL_STAY")
  )

}
