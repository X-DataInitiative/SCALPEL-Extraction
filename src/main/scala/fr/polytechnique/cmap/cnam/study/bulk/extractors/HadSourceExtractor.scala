// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, HospitalStay, MedicalAct, MedicalTakeOverReason}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.acts.HadCcamActExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses.{HadAssociatedDiagnosisExtractor, HadMainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays.HadHospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.events.takeoverreasons.{HadAssociatedTakeOverExtractor, HadMainTakeOverExtractor}

class HadSourceExtractor(override val path: String, override val saveMode: String, override val fileFormat: String)
  extends SourceExtractor(path, saveMode, fileFormat) {
  override val sourceName: String = "HAD"
  override val extractors = List(
    ExtractorSources[MedicalAct, SimpleExtractorCodes](HadCcamActExtractor(SimpleExtractorCodes.empty), List("HAD_C", "HAD_A"), "HAD_CCAM_ACT"),
    ExtractorSources[Diagnosis, SimpleExtractorCodes](HadMainDiagnosisExtractor(SimpleExtractorCodes.empty), List("HAD_C", "HAD_B"), "HAD_MAIN_DIAGNOSIS"),
    ExtractorSources[Diagnosis, SimpleExtractorCodes](HadAssociatedDiagnosisExtractor(SimpleExtractorCodes.empty), List("HAD_C", "HAD_D"), "HAD_ASSOCIATED_DIAGNOSIS"),
    ExtractorSources[MedicalTakeOverReason, SimpleExtractorCodes](
      HadMainTakeOverExtractor(SimpleExtractorCodes.empty),
      List("HAD_C", "HAD_B"),
      "HAD_MAIN_TAKE_OVER_REASON"
    ),
    ExtractorSources[MedicalTakeOverReason, SimpleExtractorCodes](
      HadAssociatedTakeOverExtractor(SimpleExtractorCodes.empty),
      List("HAD_C", "HAD_B"),
      "HAD_ASSOCIATED_TAKE_OVER_REASON"
    ),
    ExtractorSources[HospitalStay, SimpleExtractorCodes](
      HadHospitalStaysExtractor,
      List("HAD_C", "HAD_B"),
      "HAD_STAYS"
    )
  )
}