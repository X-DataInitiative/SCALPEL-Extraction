// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulkDrees.extractors

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.acts.McoCcamActExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.events.classifications.GhmExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses.{McoAssociatedDiagnosisExtractor, McoLinkedDiagnosisExtractor, McoMainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays.McoHospitalStaysExtractor

class McoSourceExtractor(override val path: String, override val saveMode: String) extends SourceExtractor(
  path,
  saveMode
) {
  override val sourceName: String = "MCO"
  override val extractors = List(
    ExtractorSources[MedicalAct, SimpleExtractorCodes](
      McoCcamActExtractor(SimpleExtractorCodes.empty),
      List("MCO_C", "MCO_A"),
      "MCO_CCAM_ACT"
    ),
    ExtractorSources[Diagnosis, SimpleExtractorCodes](
      McoMainDiagnosisExtractor(SimpleExtractorCodes.empty),
      List("MCO_C", "MCO_B"),
      "MCO_MAIN_DIAGNOSIS"
    ),
    ExtractorSources[Diagnosis, SimpleExtractorCodes](
      McoAssociatedDiagnosisExtractor(SimpleExtractorCodes.empty),
      List("MCO_C", "MCO_B", "MCO_D"),
      "MCO_ASSOCIATED_DIAGNOSIS"
    ),
    ExtractorSources[Diagnosis, SimpleExtractorCodes](
      McoLinkedDiagnosisExtractor(SimpleExtractorCodes.empty),
      List("MCO_C", "MCO_B"),
      "MCO_LINKED_DIAGNOSIS"
    ),
    ExtractorSources[HospitalStay, SimpleExtractorCodes](
      McoHospitalStaysExtractor,
      List("MCO_C", "MCO_B"),
      "MCO_HOSPITAL_STAY"
    )
//    ExtractorSources[Classification, SimpleExtractorCodes]( // not compiling due to bad GhmExtractor type
//      GhmExtractor(SimpleExtractorCodes.empty),
//      List("MCO_C", "MCO_B"),
//      "MCO_GHM"
//    )
  )
}
