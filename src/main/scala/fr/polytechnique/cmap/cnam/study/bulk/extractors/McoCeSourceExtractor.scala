// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.acts.McoCeCcamActExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays.McoceEmergenciesExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.events.ngapacts.{McoCeFbstcNgapActExtractor, McoCeFcstcNgapActExtractor, NgapActClassConfig, NgapActConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.events.prestations.{McoCeFbstcSpecialtyExtractor, McoCeFcstcSpecialtyExtractor}

class McoCeSourceExtractor(override val path: String, override val saveMode: String, override val fileFormat: String)
  extends SourceExtractor(path, saveMode, fileFormat) {
  override val sourceName: String = "MCO_CE"
  override val extractors = List(
    ExtractorSources[MedicalAct, SimpleExtractorCodes](
      McoCeCcamActExtractor(SimpleExtractorCodes.empty),
      List("MCO_CSTC", "MCO_FMSTC"),
      "MCO_CE_CCAM_ACTS"
    ),
    ExtractorSources[NgapAct, NgapActConfig[NgapActClassConfig]](
      new McoCeFbstcNgapActExtractor(NgapActConfig(List.empty)),
      List("MCO_CSTC", "MCO_FBSTC"),
      "MCO_CE_FBSTC_NGAP_ACTS"
    ),
    ExtractorSources[NgapAct, NgapActConfig[NgapActClassConfig]](
      new McoCeFcstcNgapActExtractor(NgapActConfig(List.empty)),
      List("MCO_CSTC", "MCO_FCSTC"),
      "MCO_CE_FCSTC_NGAP_ACTS"
    ),
    ExtractorSources[PractitionerClaimSpeciality, SimpleExtractorCodes](
      McoCeFbstcSpecialtyExtractor(SimpleExtractorCodes.empty),
      List("MCO_CSTC", "MCO_FBSTC"),
      "MCO_CE_FBSTC_PRACTITIONER_SPECIALITY"
    ),
    ExtractorSources[PractitionerClaimSpeciality, SimpleExtractorCodes](
      McoCeFcstcSpecialtyExtractor(SimpleExtractorCodes.empty),
      List("MCO_CSTC", "MCO_FCSTC"),
      "MCO_CE_FCSTC_PRACTITIONER_SPECIALITY"
    ),
    ExtractorSources[HospitalStay, SimpleExtractorCodes](
      McoceEmergenciesExtractor,
      List("MCO_CSTC", "MCO_FBSTC"),
      "MCO_CE_EMERGENCY_VISIT"
    )
  )
}
