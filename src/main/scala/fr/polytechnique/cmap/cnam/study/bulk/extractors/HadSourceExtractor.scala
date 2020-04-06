// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, HospitalStay, MedicalAct, MedicalTakeOverReason}
import fr.polytechnique.cmap.cnam.etl.extractors.acts.HadCcamActExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{HadAssociatedDiagnosisExtractor, HadMainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.HadHospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.takeOverReasons.{HadAssociatedTakeOverExtractor, HadMainTakeOverExtractor}

class HadSourceExtractor(override val path: String, override val saveMode: String) extends SourceExtractor(
  path,
  saveMode
) {
  override val sourceName: String = "HAD"
  override val extractors = List(
    ExtractorSources[MedicalAct](HadCcamActExtractor, List("HAD_C", "HAD_A"), "HAD_CCAM_ACT"),
    ExtractorSources[Diagnosis](HadMainDiagnosisExtractor, List("HAD_C", "HAD_B"), "HAD_MAIN_DIAGNOSIS"),
    ExtractorSources[Diagnosis](HadAssociatedDiagnosisExtractor, List("HAD_C", "HAD_D"), "HAD_ASSOCIATED_DIAGNOSIS"),
    ExtractorSources[MedicalTakeOverReason](
      HadMainTakeOverExtractor,
      List("HAD_C", "HAD_B"),
      "HAD_MAIN_TAKE_OVER_REASON"
    ),
    ExtractorSources[MedicalTakeOverReason](
      HadAssociatedTakeOverExtractor,
      List("HAD_C", "HAD_B"),
      "HAD_ASSOCIATED_TAKE_OVER_REASON"
    ),
    ExtractorSources[HospitalStay](
      HadHospitalStaysExtractor,
      List("HAD_C", "HAD_B"),
      "HAD_STAYS"
    )
  )
}